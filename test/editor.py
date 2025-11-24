# editor_optimized.py
# Original uploaded file path: /mnt/data/editor.py
# Optimized for large datasets (~365k rows).
#
# Key changes:
# - QThreadPool + QRunnable for heavy IO/search/save tasks
# - QListView + QAbstractListModel for virtualized list display
# - Filtering and loading done in background threads, model updates via signals
# - Only rows with EDITED == True are saved into final_translation.tsv
# - UI updates are guarded to avoid treating programmatic changes as manual edits

import sys
import os
import pandas as pd
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout,
    QHBoxLayout, QTextEdit, QPushButton, QListView,
    QLabel, QFileDialog, QMessageBox, QSplitter, QLineEdit
)
from PyQt5.QtCore import (
    Qt, QSettings, QCoreApplication, QMimeData,
    QAbstractListModel, QModelIndex, pyqtSignal, QObject,
    QRunnable, QThreadPool
)
from PyQt5.QtGui import QFont

# QSettings info
QCoreApplication.setOrganizationName("wmmTranslate")
QCoreApplication.setApplicationName("TranslationEditor")

# keys
LAST_ID_KEY = 'last_processed_id'
ENG_FILE_KEY = 'eng_tsv_path'
RUS_FILE_KEY = 'rus_tsv_path'
FINAL_FILE_KEY = 'final_tsv_path'
DICTIONARY_FILE_KEY = 'dictionary_tsv_path'


class PlainTextEdit(QTextEdit):
    def insertFromMimeData(self, source: QMimeData):
        if source.hasHtml() or source.hasFormat("text/rtf"):
            self.insertPlainText(source.text())
        else:
            super().insertFromMimeData(source)


# -----------------------
# Worker utilities
# -----------------------
class WorkerSignals(QObject):
    finished = pyqtSignal(object)  # pass any python object as result
    error = pyqtSignal(Exception)


class RunnableTask(QRunnable):
    """
    Generic QRunnable wrapper. `fn` should be a callable that returns a result (or raises).
    Signals: finished(result) or error(exception)
    """
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    def run(self):
        try:
            res = self.fn(*self.args, **self.kwargs)
            self.signals.finished.emit(res)
        except Exception as e:
            self.signals.error.emit(e)


# -----------------------
# List model for virtualized display
# -----------------------
class TranslationListModel(QAbstractListModel):
    def __init__(self, df_ref, filtered_indices):
        super().__init__()
        # df_ref: reference to the DataFrame object (must be kept consistent)
        self.df_ref = df_ref
        # filtered_indices: list of integer indices into df_ref (i.e., df.index positions)
        self.filtered_indices = filtered_indices if filtered_indices is not None else []

    def rowCount(self, parent=QModelIndex()):
        return len(self.filtered_indices)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or role != Qt.DisplayRole:
            return None
        idx = self.filtered_indices[index.row()]
        # read from df directly (should be fast, single-row access)
        try:
            row = self.df_ref.loc[idx]
            return f"{row['ID']} | {row['DISPLAY_TEXT']}"
        except Exception:
            return ""

    def update_indices(self, new_indices):
        # Reset model to show new list (efficient for large lists)
        self.beginResetModel()
        self.filtered_indices = new_indices
        self.endResetModel()

    def get_df_index(self, list_row):
        """Return the DataFrame index for a given list row"""
        if 0 <= list_row < len(self.filtered_indices):
            return self.filtered_indices[list_row]
        return None


# -----------------------
# Main editor
# -----------------------
class TranslationEditor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("🎮 Редактор Переводов TSV (Optimized)")
        self.setGeometry(100, 100, 1200, 800)

        self.settings = QSettings(QCoreApplication.organizationName(), QCoreApplication.applicationName())

        # state file paths
        self.eng_file_path = self.settings.value(ENG_FILE_KEY, "") or ""
        self.rus_file_path = self.settings.value(RUS_FILE_KEY, "") or ""
        self.final_file_path = self.settings.value(FINAL_FILE_KEY, "") or ""
        self.dict_file_path = self.settings.value(DICTIONARY_FILE_KEY, "") or ""

        # flags
        self.eng_file_loaded = False
        self.rus_file_loaded = False
        self.current_df_index = None  # actual pandas index (not list row)
        self.updating_ui = False  # prevent programmatic changes from marking EDITED

        # Data containers
        # We'll keep DataFrame indexed by a simple RangeIndex for performance; ID stored as column
        self.df = pd.DataFrame(columns=['ID', 'ENG', 'RUS_MACHINE', 'RUS_EDITED', 'DISPLAY_TEXT', 'EDITED'])
        # dictionary DataFrame
        self.dict_df = pd.DataFrame(columns=['Source', 'Target'])

        # filtered indices = list of df.index (integers) that are currently shown by model
        self.filtered_indices = []

        # threading pool
        self.pool = QThreadPool.globalInstance()

        self.setup_ui()

    def setup_ui(self):
        central_widget = QWidget()
        main_layout = QHBoxLayout(central_widget)

        # left panel
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setAlignment(Qt.AlignTop)

        left_layout.addWidget(QLabel("### 📁 Файлы Источников"))

        self.select_eng_button = QPushButton("🇬🇧 Указать ENG TSV")
        self.select_eng_button.clicked.connect(lambda: self.select_tsv_file('ENG'))
        self.eng_file_label = QLabel(f"ENG: {os.path.basename(self.eng_file_path) if self.eng_file_path else 'Не выбран'}")

        left_layout.addWidget(self.select_eng_button)
        left_layout.addWidget(self.eng_file_label)

        self.select_rus_button = QPushButton("🇷🇺 Указать RUS TSV")
        self.select_rus_button.clicked.connect(lambda: self.select_tsv_file('RUS'))
        self.rus_file_label = QLabel(f"RUS: {os.path.basename(self.rus_file_path) if self.rus_file_path else 'Не выбран'}")

        left_layout.addWidget(self.select_rus_button)
        left_layout.addWidget(self.rus_file_label)

        left_layout.addWidget(QLabel("---"))

        left_layout.addWidget(QLabel("### 🎯 Файл Назначения (Итоговый)"))
        self.select_final_button = QPushButton("💾 Указать / Создать Файл Назначения")
        self.select_final_button.clicked.connect(self.select_final_file)
        self.final_file_label = QLabel(f"Файл: {os.path.basename(self.final_file_path) if self.final_file_path else 'Не выбран'}")
        left_layout.addWidget(self.select_final_button)
        left_layout.addWidget(self.final_file_label)

        left_layout.addWidget(QLabel("---"))

        left_layout.addWidget(QLabel("### 📚 Файл Словаря (Подсказки)"))
        self.select_dict_button = QPushButton("📚 Указать Файл Словаря")
        self.select_dict_button.clicked.connect(self.select_dictionary_file)
        self.dict_file_label = QLabel(f"Словарь: {os.path.basename(self.dict_file_path) if self.dict_file_path else 'Не выбран'}")
        left_layout.addWidget(self.select_dict_button)
        left_layout.addWidget(self.dict_file_label)

        left_layout.addWidget(QLabel("---"))

        self.load_all_button = QPushButton("🚀 Загрузить все данные и начать работу (в фоне)")
        self.load_all_button.clicked.connect(self.load_all_data)
        left_layout.addWidget(self.load_all_button)

        left_layout.addWidget(QLabel("---"))

        self.progress_label = QLabel("Прогресс: 0/0 (0% завершено)")
        left_layout.addWidget(self.progress_label)

        left_layout.addWidget(QLabel("### 🔎 Поиск:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Введите текст для поиска...")
        # debounce search: connect to function that starts a background filter
        self.search_input.textChanged.connect(self.on_search_text_changed)
        left_layout.addWidget(self.search_input)

        # Use QListView with custom model
        self.list_view = QListView()
        self.list_view.setFont(QFont("Arial", 10))
        self.list_view.setMinimumWidth(300)
        self.list_view.clicked.connect(self.on_list_view_clicked)  # will get QModelIndex
        left_layout.addWidget(self.list_view)

        left_layout.addStretch(1)

        # right panel
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)

        top_text_layout = QHBoxLayout()

        eng_group = QVBoxLayout()
        eng_group.addWidget(QLabel("### 🇬🇧 Оригинал (ENG):"))
        self.eng_text_edit = QTextEdit()
        self.eng_text_edit.setReadOnly(True)
        self.eng_text_edit.setMaximumHeight(150)
        eng_group.addWidget(self.eng_text_edit)
        top_text_layout.addLayout(eng_group)

        rus_machine_group = QVBoxLayout()
        rus_machine_group.addWidget(QLabel("### 🤖 Машинный Перевод (RUS_MACHINE):"))
        self.rus_machine_text_edit = QTextEdit()
        self.rus_machine_text_edit.setReadOnly(True)
        self.rus_machine_text_edit.setMaximumHeight(150)
        rus_machine_group.addWidget(self.rus_machine_text_edit)
        top_text_layout.addLayout(rus_machine_group)
        right_layout.addLayout(top_text_layout)

        self.dictionary_label = QLabel("### 📘 Подсказки из Словаря:")
        self.dictionary_results = QTextEdit()
        self.dictionary_results.setReadOnly(True)
        self.dictionary_results.setMaximumHeight(120)
        self.dictionary_results.setFont(QFont("Arial", 10))
        right_layout.addWidget(self.dictionary_label)
        right_layout.addWidget(self.dictionary_results)

        right_layout.addWidget(QLabel("### ✏️ Редактирование (RUS_EDITED):"))
        self.rus_edited_text_edit = PlainTextEdit()
        self.rus_edited_text_edit.setFont(QFont("Arial", 12))
        right_layout.addWidget(self.rus_edited_text_edit)
        # connect manual edit handler
        self.rus_edited_text_edit.textChanged.connect(self.on_rus_edited_changed)

        copy_layout = QHBoxLayout()
        copy_eng_btn = QPushButton("📋 Копировать ENG в Адаптацию")
        copy_eng_btn.clicked.connect(self.copy_eng_to_edited)
        copy_rus_btn = QPushButton("📋 Копировать RUS_MACHINE в Адаптацию")
        copy_rus_btn.clicked.connect(self.copy_machine_to_edited)
        copy_layout.addWidget(copy_eng_btn)
        copy_layout.addWidget(copy_rus_btn)
        right_layout.addLayout(copy_layout)

        right_layout.addWidget(QLabel("---"))
        button_layout = QHBoxLayout()
        self.skip_button = QPushButton("⏩ Пропустить (Взять RUS_MACHINE)")
        self.skip_button.clicked.connect(self.skip_and_next)
        self.continue_button = QPushButton("✅ Сохранить и Продолжить")
        self.continue_button.clicked.connect(self.save_and_next)
        button_layout.addWidget(self.skip_button)
        button_layout.addWidget(self.continue_button)
        right_layout.addLayout(button_layout)
        right_layout.addStretch(1)

        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([350, 850])

        main_layout.addWidget(splitter)
        self.setCentralWidget(central_widget)

        # create initial (empty) model
        self.model = TranslationListModel(self.df, self.filtered_indices)
        self.list_view.setModel(self.model)

    # -----------------------
    # UI actions
    # -----------------------
    def select_dictionary_file(self):
        initial_dir = os.path.dirname(self.dict_file_path) if self.dict_file_path else os.getcwd()
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Выберите Файл Словаря (dictionary.tsv)",
            initial_dir,
            "TSV Files (*.tsv);;All Files (*)"
        )
        if file_name:
            self.dict_file_path = file_name
            self.settings.setValue(DICTIONARY_FILE_KEY, file_name)
            self.dict_file_label.setText(f"Словарь: {os.path.basename(file_name)}")

    def select_final_file(self):
        initial_dir = os.path.dirname(self.final_file_path) if self.final_file_path else os.getcwd()
        file_name, _ = QFileDialog.getSaveFileName(
            self, "Выберите или создайте Файл Назначения",
            os.path.join(initial_dir, "final_translation.tsv"),
            "TSV Files (*.tsv);;All Files (*)"
        )
        if file_name:
            self.final_file_path = file_name
            self.settings.setValue(FINAL_FILE_KEY, file_name)
            self.final_file_label.setText(f"Файл: {os.path.basename(file_name)}")

    def select_tsv_file(self, file_type):
        initial_dir = os.path.dirname(self.eng_file_path) if self.eng_file_path else os.getcwd()
        file_name, _ = QFileDialog.getOpenFileName(
            self, f"Выберите {file_type} TSV-файл",
            initial_dir,
            "TSV Files (*.tsv);;All Files (*)"
        )
        if file_name:
            if file_type == 'ENG':
                self.eng_file_path = file_name
                self.settings.setValue(ENG_FILE_KEY, file_name)
                self.eng_file_label.setText(f"ENG: {os.path.basename(file_name)}")
            elif file_type == 'RUS':
                self.rus_file_path = file_name
                self.settings.setValue(RUS_FILE_KEY, file_name)
                self.rus_file_label.setText(f"RUS: {os.path.basename(file_name)}")

    # -----------------------
    # Loading pipeline (background)
    # -----------------------
    def load_all_data(self):
        # Basic checks
        if not self.eng_file_path or not os.path.exists(self.eng_file_path) or \
           not self.rus_file_path or not os.path.exists(self.rus_file_path) or \
           not self.final_file_path:
            QMessageBox.warning(self, "Ошибка", "Убедитесь, что пути к ENG, RUS и финальному файлу указаны и файлы существуют.")
            return

        self.load_all_button.setEnabled(False)
        # Chain tasks: load dict -> load eng -> load rus -> load final -> finish
        # We'll just start eng load; subsequent steps are in callbacks.
        task = RunnableTask(self._load_eng_task, self.eng_file_path)
        task.signals.finished.connect(self._on_eng_loaded)
        task.signals.error.connect(self._on_task_error)
        self.pool.start(task)

    def _load_eng_task(self, file_path):
        # Read only first two columns
        # Use low_memory=False to avoid dtype warnings
        df_new = pd.read_csv(file_path, sep='\t', header=None, usecols=[0, 1],
                             dtype=str, encoding='utf-8', low_memory=False)
        df_new = df_new.fillna('')
        # Build DataFrame with required columns
        df = pd.DataFrame({
            'ID': df_new.iloc[:, 0].astype(str),
            'ENG': df_new.iloc[:, 1].astype(str),
            'RUS_MACHINE': [''] * len(df_new),
            'RUS_EDITED': [''] * len(df_new),
            'DISPLAY_TEXT': df_new.iloc[:, 1].astype(str).str.slice(0, 80).str.replace('\n', ' ').str.strip() + "...",
            'EDITED': [False] * len(df_new)
        })
        # reset index to simple RangeIndex and return
        df.reset_index(drop=True, inplace=True)
        return df

    def _on_eng_loaded(self, df):
        # set main df
        self.df = df
        self.eng_file_loaded = True

        # load dictionary (in background)
        dict_task = RunnableTask(self._load_dict_task, self.dict_file_path)
        dict_task.signals.finished.connect(self._on_dict_loaded)
        dict_task.signals.error.connect(self._on_task_error)
        self.pool.start(dict_task)

        # start loading rus
        rus_task = RunnableTask(self._load_rus_task, self.rus_file_path)
        rus_task.signals.finished.connect(self._on_rus_loaded)
        rus_task.signals.error.connect(self._on_task_error)
        self.pool.start(rus_task)

    def _load_dict_task(self, dict_path):
        if not dict_path or not os.path.exists(dict_path):
            return pd.DataFrame(columns=['Source', 'Target'])
        df_d = pd.read_csv(dict_path, sep='\t', header=None, usecols=[0, 1], dtype=str, encoding='utf-8', low_memory=False)
        df_d = df_d.fillna('').iloc[:, :2]
        df_d.columns = ['Source', 'Target']
        df_d = df_d.drop_duplicates(subset=['Source']).reset_index(drop=True)
        # Also create lowercase column if needed (for quick match)
        df_d['Source_lc'] = df_d['Source'].str.lower()
        return df_d

    def _on_dict_loaded(self, dict_df):
        self.dict_df = dict_df

    def _load_rus_task(self, file_path):
        # read first two columns: id and text
        df_rus = pd.read_csv(file_path, sep='\t', header=None, usecols=[0, 1], dtype=str, encoding='utf-8', low_memory=False)
        df_rus = df_rus.fillna('')
        df_rus.reset_index(drop=True, inplace=True)
        # return mapping dict id->text as Series to merge quickly
        mapping = pd.Series(df_rus.iloc[:, 1].astype(str).values, index=df_rus.iloc[:, 0].astype(str).values)
        return mapping

    def _on_rus_loaded(self, mapping_series):
        if self.df.empty:
            # nothing to merge against
            self.rus_file_loaded = True
            self.load_final_file_content_background()
            return
        # Map RUS_MACHINE by ID — efficient via pandas indexing
        # Create a Series of ENG IDs and map
        ids = self.df['ID'].astype(str)
        self.df['RUS_MACHINE'] = ids.map(mapping_series).fillna('').astype(str)
        # Don't auto-fill RUS_EDITED; keep empty unless previously saved
        self.rus_file_loaded = True
        # Now load final file
        self.load_final_file_content_background()

    def load_final_file_content_background(self):
        # read final file in background
        task = RunnableTask(self._load_final_task, self.final_file_path)
        task.signals.finished.connect(self._on_final_loaded)
        task.signals.error.connect(self._on_task_error)
        self.pool.start(task)

    def _load_final_task(self, final_path):
        if not final_path or not os.path.exists(final_path):
            return None
        df_final = pd.read_csv(final_path, sep='\t', encoding='utf-8', dtype=str, low_memory=False)
        df_final = df_final.fillna('')
        return df_final

    def _on_final_loaded(self, df_final):
        # If there is a final file, merge RUS_EDITED and mark EDITED=True for those rows
        if isinstance(df_final, pd.DataFrame) and not df_final.empty:
            if 'ID' in df_final.columns and 'RUS_EDITED' in df_final.columns:
                df_map = df_final.set_index('ID')['RUS_EDITED'].to_dict()
                ids = self.df['ID'].astype(str)
                # update RUS_EDITED for matching IDs
                self.df['RUS_EDITED'] = ids.map(df_map).fillna(self.df['RUS_EDITED']).astype(str)
                # mark EDITED for rows where final file contained non-empty RUS_EDITED
                self.df['EDITED'] = self.df.apply(
                    lambda r: True if (r['RUS_EDITED'] and r['RUS_EDITED'] != '') else False,
                    axis=1
                )
        # Setup filtered_indices to full set
        self.filtered_indices = list(self.df.index)  # full range
        # update model
        self.model.df_ref = self.df
        self.model.update_indices(self.filtered_indices)
        # load progress (non-blocking)
        self.load_progress()
        self.load_all_button.setEnabled(False)
        QMessageBox.information(self, "Готово", "Все данные загружены (в фоне). Можно начинать работу.")

    def _on_task_error(self, exc):
        QMessageBox.critical(self, "Ошибка фоновой задачи", str(exc))
        self.load_all_button.setEnabled(True)

    # -----------------------
    # Search (backgrounded)
    # -----------------------
    def on_search_text_changed(self, text):
        # schedule background filter — quick debounce: start small background task
        # For large datasets vectorized filtering with pandas is fast, but we do it in background
        task = RunnableTask(self._filter_task, text)
        task.signals.finished.connect(self._on_filter_finished)
        task.signals.error.connect(self._on_task_error)
        self.pool.start(task)

    def _filter_task(self, text):
        if self.df.empty:
            return list()
        t = str(text).strip()
        if t == "":
            # return full indices
            return list(self.df.index)
        # create mask vectorized (case-insensitive)
        # To speed up, use Series.str.contains with na=False
        mask = (
            self.df['ID'].astype(str).str.contains(t, case=False, na=False) |
            self.df['ENG'].astype(str).str.contains(t, case=False, na=False) |
            self.df['RUS_MACHINE'].astype(str).str.contains(t, case=False, na=False) |
            self.df['RUS_EDITED'].astype(str).str.contains(t, case=False, na=False)
        )
        # get indices where mask True
        return self.df.index[mask].tolist()

    def _on_filter_finished(self, indices):
        # update filtered indices in model
        self.filtered_indices = indices
        self.model.update_indices(self.filtered_indices)
        # set current row/index to first if present
        if self.filtered_indices:
            self.current_df_index = self.filtered_indices[0]
            # update UI for that row
            self.update_ui_for_current_row()
            # select first in list view
            self.list_view.setCurrentIndex(self.model.index(0))
        else:
            self.current_df_index = None
            self.update_ui_for_current_row()

    # -----------------------
    # UI interactions
    # -----------------------
    def on_list_view_clicked(self, qmodelindex):
        if not qmodelindex.isValid():
            return
        list_row = qmodelindex.row()
        df_idx = self.model.get_df_index(list_row)
        if df_idx is None:
            return
        self.current_df_index = df_idx
        self.update_ui_for_current_row()

    def update_ui_for_current_row(self):
        # If nothing loaded or no selection, clear fields
        if self.df.empty or self.current_df_index is None:
            self.updating_ui = True
            self.eng_text_edit.clear()
            self.rus_machine_text_edit.clear()
            self.rus_edited_text_edit.clear()
            self.dictionary_results.clear()
            self.dictionary_label.setText("### 📘 Подсказки из Словаря:")
            self.updating_ui = False
            self.update_progress_label()
            return

        row = self.df.loc[self.current_df_index]
        # set texts
        self.eng_text_edit.setPlainText(row['ENG'])
        self.rus_machine_text_edit.setPlainText(row['RUS_MACHINE'])

        # dictionary hints: check dictionary sources in current eng string
        hints = []
        eng_lc = str(row['ENG']).lower()
        if not self.dict_df.empty:
            # iterate dict rows — 1k checks per displayed row is acceptable
            for _, drow in self.dict_df.iterrows():
                src = drow['Source_lc'] if 'Source_lc' in drow else str(drow['Source']).lower()
                if src and src in eng_lc:
                    hints.append(f"• {drow['Source']} ➡ {drow['Target']}")
        if hints:
            self.dictionary_results.setPlainText("\n".join(hints))
            self.dictionary_label.setText(f"### 📘 Подсказки из Словаря (Найдено: {len(hints)}):")
        else:
            self.dictionary_results.clear()
            self.dictionary_label.setText("### 📘 Подсказки из Словаря:")

        # show RUS_EDITED if present, otherwise show machine (but do not set EDITED)
        show_text = row['RUS_EDITED'] if row['RUS_EDITED'] else row['RUS_MACHINE']
        self.updating_ui = True
        # setText rather than setPlainText to avoid extra signals? use setPlainText
        self.rus_edited_text_edit.setPlainText(show_text)
        self.updating_ui = False
        self.update_progress_label()

    def copy_eng_to_edited(self):
        if self.current_df_index is None:
            return
        text = self.df.at[self.current_df_index, 'ENG']
        # programmatic change; do not mark EDITED yet (user must confirm Save)
        self.updating_ui = True
        self.rus_edited_text_edit.setPlainText(text)
        self.updating_ui = False
        # update memory RUS_EDITED (not marked EDITED until Save)
        self.df.at[self.current_df_index, 'RUS_EDITED'] = text
        self.update_progress_label()

    def copy_machine_to_edited(self):
        if self.current_df_index is None:
            return
        text = self.df.at[self.current_df_index, 'RUS_MACHINE']
        self.updating_ui = True
        self.rus_edited_text_edit.setPlainText(text)
        self.updating_ui = False
        # store in memory but not mark EDITED
        self.df.at[self.current_df_index, 'RUS_EDITED'] = text
        self.update_progress_label()

    def on_rus_edited_changed(self):
        # Called on every small change; we only update in-memory RUS_EDITED and EDITED flag
        if self.updating_ui:
            return
        if self.df.empty or self.current_df_index is None:
            return
        current_machine = self.df.at[self.current_df_index, 'RUS_MACHINE']
        new_text = self.rus_edited_text_edit.toPlainText()
        # store in df
        self.df.at[self.current_df_index, 'RUS_EDITED'] = new_text
        # mark edited only if differs from machine and not empty
        is_edited = (new_text != current_machine) and (new_text != '')
        self.df.at[self.current_df_index, 'EDITED'] = bool(is_edited)
        self.update_progress_label()

    # -----------------------
    # Save / Skip / Progress logic
    # -----------------------
    def save_and_next(self):
        """Saves current edited text as confirmed (EDITED=True) and writes final file in background"""
        if self.df.empty or self.current_df_index is None or not self.final_file_path:
            QMessageBox.warning(self, "Ошибка", "Данные не загружены или не выбран файл назначения.")
            return
        # mark current row as confirmed by user
        self.df.at[self.current_df_index, 'RUS_EDITED'] = self.rus_edited_text_edit.toPlainText()
        self.df.at[self.current_df_index, 'EDITED'] = True
        # save last id
        current_id = self.df.at[self.current_df_index, 'ID']
        self.settings.setValue(LAST_ID_KEY, current_id)
        # write final file in background
        task = RunnableTask(self._save_final_task, self.final_file_path, self.df)
        task.signals.finished.connect(lambda _: self._after_save_next(True))
        task.signals.error.connect(self._on_task_error)
        self.pool.start(task)

    def skip_and_next(self):
        """Copies machine translation into RUS_EDITED in memory but does NOT mark EDITED"""
        if self.df.empty or self.current_df_index is None or not self.final_file_path:
            QMessageBox.warning(self, "Ошибка", "Данные не загружены или не выбран файл назначения.")
            return
        # set RUS_EDITED to machine text but keep EDITED flag unchanged (so it won't be saved)
        self.df.at[self.current_df_index, 'RUS_EDITED'] = self.df.at[self.current_df_index, 'RUS_MACHINE']
        # save last id
        current_id = self.df.at[self.current_df_index, 'ID']
        self.settings.setValue(LAST_ID_KEY, current_id)
        # still write final file (only rows with EDITED==True will be saved) to keep file up-to-date
        task = RunnableTask(self._save_final_task, self.final_file_path, self.df)
        task.signals.finished.connect(lambda _: self._after_save_next(True))
        task.signals.error.connect(self._on_task_error)
        self.pool.start(task)

    def _save_final_task(self, final_path, df_snapshot):
        """
        Save only rows where EDITED == True.
        df_snapshot is a reference to self.df (we'll take a shallow copy of necessary columns)
        This function runs in background.
        """
        # Create df_to_save as minimal copy
        df_to_save = df_snapshot.loc[df_snapshot['EDITED'] == True, ['ID', 'RUS_EDITED']].copy()
        # If empty, write header only to ensure file exists
        if df_to_save.empty:
            pd.DataFrame(columns=['ID', 'RUS_EDITED']).to_csv(final_path, sep='\t', index=False, encoding='utf-8')
        else:
            df_to_save.to_csv(final_path, sep='\t', index=False, encoding='utf-8')
        return True

    def _after_save_next(self, success):
        # navigate to next index in filtered_indices
        if not success:
            return
        if not self.filtered_indices:
            return
        if self.current_df_index is None:
            # pick first
            self.current_df_index = self.filtered_indices[0]
            self.update_ui_for_current_row()
            return
        try:
            pos = self.filtered_indices.index(self.current_df_index)
            next_pos = (pos + 1) % len(self.filtered_indices)
            self.current_df_index = self.filtered_indices[next_pos]
        except ValueError:
            # current not found: reset to first
            self.current_df_index = self.filtered_indices[0]
        # update UI
        self.update_ui_for_current_row()
        # highlight in view
        self.list_view.setCurrentIndex(self.model.index(self.filtered_indices.index(self.current_df_index)))

    # -----------------------
    # Progress and load progress
    # -----------------------
    def load_progress(self):
        if self.df.empty:
            return
        last_id = self.settings.value(LAST_ID_KEY, None)
        if last_id is not None:
            # find index by ID value (fast: using vectorized comparison)
            matches = self.df.index[self.df['ID'].astype(str) == str(last_id)].tolist()
            if matches:
                # go to next element after the one in file
                idx = matches[0] + 1
                if idx >= len(self.df):
                    idx = 0
                self.current_df_index = idx
            else:
                self.current_df_index = 0
        else:
            self.current_df_index = 0
        # Ensure filtered indices set to full if empty
        if not self.filtered_indices:
            self.filtered_indices = list(self.df.index)
            self.model.update_indices(self.filtered_indices)
        self.update_ui_for_current_row()
        # select in list view
        try:
            list_row = self.filtered_indices.index(self.current_df_index)
            self.list_view.setCurrentIndex(self.model.index(list_row))
        except Exception:
            pass

    def update_progress_label(self):
        if self.df.empty:
            self.progress_label.setText("Прогресс: 0/0")
            return
        total_rows = len(self.df)
        processed_count = int(self.df['EDITED'].sum())
        pos = (self.df.index.get_loc(self.current_df_index) + 1) if (self.current_df_index is not None and self.current_df_index in self.df.index) else 0
        self.progress_label.setText(f"Прогресс: {pos}/{total_rows} (Отредактировано: {processed_count} из {total_rows})")

    # -----------------------
    # Close event saves last position
    # -----------------------
    def closeEvent(self, event):
        if self.current_df_index is not None and not self.df.empty:
            current_id = self.df.at[self.current_df_index, 'ID']
            self.settings.setValue(LAST_ID_KEY, current_id)
        event.accept()


if __name__ == '__main__':
    # quick pandas presence check
    try:
        pd.DataFrame()
    except Exception:
        print("Ошибка: библиотека pandas не найдена. Установите: pip install pandas")
        sys.exit(1)

    app = QApplication(sys.argv)
    editor = TranslationEditor()
    editor.show()
    sys.exit(app.exec_())
