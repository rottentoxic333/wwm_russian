# .github/scripts/validate_translation.py
#!/usr/bin/env python3

"""
Скрипт валидации файла переводов (translation_ru.tsv)

ПРОВЕРЯЕТ:
  1. Формат файла — каждая строка должна содержать ID и текст, разделенные TAB
  2. Уникальность ID — проверяет на дубликаты ID в файле
  3. Формат ID — ID должен содержать ровно 16 символов (hex: 0-9, a-f)
  4. Целостность техтегов — любые слова со знаком "_" (например, "Object_12", "Skill_Name")
     не должны быть переведены (остаются как есть)
  5. Пустые переводы — убеждается, что для каждого ID есть текст перевода

СТРУКТУРА ФАЙЛА:
  ID (16 hex)  \t  Текст перевода
  
  Пример:
  a0efdcb60026c4cd	Ряса монаха, доспех воина, одежды ученого

ТЕГИ И ПАРАМЕТРЫ (разрешены):
  - Параметры вида <текст|значение|параметр> — внутри можно переводить текст
  - Управляющие символы \n, \r, \t — сохраняются без изменений
  - Цветовые коды #Y, #R, #E и т.д. — остаются на месте
  - Плейсхолдеры вида <%s>, <{}>, {0} — не трогаются

ВОЗВРАЩАЕТ:
  - 0 (успех) если ошибок нет
  - 1 (ошибка) если найдены критические проблемы

ИСПОЛЬЗОВАНИЕ:
  python validate_translation.py translation_ru.tsv
"""

import sys
import re
from collections import defaultdict

def validate_tsv(filepath):
    errors = []
    warnings = []
    seen_ids = defaultdict(list)
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"❌ Файл не найден: {filepath}")
        sys.exit(1)
        
    # Убираем возможный BOM (UTF-8 BOM: \ufeff) и переводы строк
    header = lines[0].lstrip('\ufeff').rstrip('\n\r')
    if not header.startswith('ID\tOriginalText'):
        errors.append(
            f"❌ Неверный заголовок. Ожидается: 'ID\\tOriginalText', получено: '{header[:50]}'"
        )
    
    # Пропускаем заголовок
    lines = lines[1:]
    
    for line_num, line in enumerate(lines, 1):
        line = line.rstrip('\n')
        if not line.strip(): continue
        
        # Проверка структуры TSV
        parts = line.split('\t', 1)
        if len(parts) != 2:
            if '\t' not in line:
                errors.append(f"Строка {line_num}: нет TAB. Формат: ID\\tТекст")
            else:
                errors.append(f"Строка {line_num}: неверный формат")
            continue
        
        id_str, text = parts
        
        # Проверка ID
        if not re.match(r'^[a-f0-9]{16}$', id_str):
            warnings.append(f"Строка {line_num}: странный ID '{id_str}'")
        
        # Проверка дубликатов ID
        if id_str in seen_ids:
            errors.append(f"Строка {line_num}: дубликат ID '{id_str}' (был в строке {seen_ids[id_str]})")
        else:
            seen_ids[id_str].append(line_num)
        
        # Проверка техтегов (слова с подчеркиванием)
        underscored_words = re.findall(r'\b\w+_\w+(?:_\d+)?\b', line)
        if underscored_words:
            for word in underscored_words:
                if re.match(r'^[а-яёА-ЯЁ]', word):
                    errors.append(f"Строка {line_num}: переведен техтег '{word}'. Теги с '_' нельзя менять!")
        
        # Проверка на пустой текст
        if not text.strip():
            errors.append(f"Строка {line_num}: пустой перевод для ID '{id_str}'")

    # Вывод итогов
    print(f"\n📋 Проверка файла: {filepath}")
    print(f"📊 Всего строк: {len(lines)}")
    
    if errors:
        print(f"\n❌ ОШИБКИ ({len(errors)}):")
        for error in errors:
            print(f"   {error}")
    
    if warnings:
        print(f"\n⚠️  ПРЕДУПРЕЖДЕНИЯ ({len(warnings)}):")
        for warning in warnings:
            print(f"   {warning}")
    
    if not errors and not warnings:
        print("\n✅ Все проверки пройдены успешно!")
        return 0
    
    return 1 if errors else 0

if __name__ == '__main__':
    filepath = sys.argv[1] if len(sys.argv) > 1 else 'translation_ru.tsv'
    exit_code = validate_tsv(filepath)
    sys.exit(exit_code)
