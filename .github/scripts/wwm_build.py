#!/usr/bin/env python3
"""
Полный скрипт для автоматизации перевода WWM (Multi-file версия)
Распаковка → Извлечение текстов → Применение перевода → Запеканье .dat файлов
Поддерживает несколько файлов игры с одним общим переводом
Готово для CI/CD и GitHub релизов!
"""

import argparse
import os
import sys
import struct
import pyzstd
import csv
import re


def log(msg):
    print(f"[WWM] {msg}")


def extract_file(input_file, output_dir):
    try:
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_subdir = os.path.join(output_dir, base_name)
        os.makedirs(output_subdir, exist_ok=True)

        with open(input_file, 'rb') as f:
            if f.read(4) != b'\xEF\xBE\xAD\xDE':
                log(f"❌ Неверный формат файла: {input_file}")
                return False

            f.read(4)
            offset_count_bytes = f.read(4)
            offset_count = struct.unpack('<I', offset_count_bytes)[0] + 1

            if offset_count == 1:
                comp_block_len = struct.unpack('<I', f.read(4))[0]
                comp_block = f.read(comp_block_len)

                if len(comp_block) < comp_block_len:
                    return False

                header = comp_block[:9]
                comp_data_part = comp_block[9:]

                if len(header) < 9:
                    return False
                    
                comp_type, comp_size, decomp_size = struct.unpack('<BII', header)

                if comp_type == 0x04:
                    try:
                        decomp_data = pyzstd.decompress(comp_data_part)
                        output_path = os.path.join(output_subdir, f"{base_name}_0.dat")
                        with open(output_path, 'wb') as outf:
                            outf.write(decomp_data)
                    except Exception as e:
                        log(f"⚠️  Ошибка распаковки блока 0: {e}")
            else:
                offsets = [struct.unpack('<I', f.read(4))[0] for _ in range(offset_count)]
                data_start = f.tell()
                
                for i in range(offset_count):
                    current_offset = offsets[i]
                    
                    if i == (offset_count - 1):
                        continue
                    
                    next_offset = offsets[i + 1]
                    block_len = next_offset - current_offset

                    f.seek(data_start + current_offset)
                    comp_block = f.read(block_len)

                    if len(comp_block) < block_len:
                        continue

                    if len(comp_block) < 9:
                        continue

                    header = comp_block[:9]
                    comp_data_part = comp_block[9:]
                    comp_type, comp_size, decomp_size = struct.unpack('<BII', header)
                    
                    if comp_type == 0x04:
                        try:
                            decomp_data = pyzstd.decompress(comp_data_part)
                            output_path = os.path.join(output_subdir, f"{base_name}_{i}.dat")
                            with open(output_path, 'wb') as outf:
                                outf.write(decomp_data)
                        except Exception as e:
                            log(f"⚠️  Ошибка распаковки блока {i}: {e}")

            log(f"✅ Распаковка завершена: {output_subdir}")
            return output_subdir

    except Exception as e:
        log(f"❌ Ошибка распаковки: {e}")
        import traceback
        traceback.print_exc()
        return False


def extract_text(input_dir, output_dir, file_prefix):
    try:
        output_path = os.path.join(output_dir, f"TextExtractor_{file_prefix}.csv")
        
        if os.path.exists(output_path):
            os.remove(output_path)
            log(f"🗑️  Удален старый файл: {output_path}")
        
        with open(output_path, 'w', encoding='utf-8', newline='') as outf:
            writer = csv.writer(outf, delimiter=';')
            writer.writerow(["Number", "File", "All Blocks", "Work Blocks", "Current Block", "Unknown", "ID", "OriginalText"])
            
            k = 0
            for filename in sorted(os.listdir(input_dir)):
                if not filename.endswith('.dat'):
                    continue
                
                if filename.endswith('_0.dat'):
                    log(f"⏭️  Пропущен оригинальный блок: {filename}")
                    continue
                
                full_path = os.path.join(input_dir, filename)
                
                try:
                    with open(full_path, 'rb') as f:
                        f.seek(0)
                        count_full = struct.unpack('<I', f.read(4))[0]
                        f.read(4)
                        count_text = struct.unpack('<I', f.read(4))[0]
                        f.read(12)
                        code = f.read(count_full).hex()
                        f.read(17)
                        data_start = f.tell()
                        
                        for i in range(count_full):
                            f.seek(data_start + (i * 16))
                            id_bytes = f.read(8).hex()
                            start_text_offset = f.tell()
                            offset_text = struct.unpack('<I', f.read(4))[0]
                            length = struct.unpack('<I', f.read(4))[0]
                            
                            f.seek(start_text_offset + offset_text)
                            text = f.read(length).decode('utf-8', errors='ignore')
                            
                            text = text.replace('\n', '\\n')
                            text = text.replace('\r', '\\r')
                            
                            k += 1
                            writer.writerow([str(k), filename, count_full, count_text, str(i), code[i*2:(i+1)*2], id_bytes, text])
                except Exception as e:
                    log(f"⚠️  Ошибка при чтении {filename}: {e}")
                    continue
        
        log(f"✅ Текстовый файл создан: {output_path} ({k} записей)")
        return output_path
    except Exception as e:
        log(f"❌ Ошибка извлечения: {e}")
        import traceback
        traceback.print_exc()
        return None


def apply_translation(tsv_path, csv_path, output_csv_path):
    try:
        translations = {}
        with open(tsv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            next(reader, None)
            for row in reader:
                if len(row) >= 2:
                    translations[row[0].strip()] = row[1].strip()
        
        log(f"✅ Загружено переводов: {len(translations)}")
        
        replaced = 0
        total = 0
        with open(csv_path, 'r', encoding='utf-8', newline='') as src, \
             open(output_csv_path, 'w', encoding='utf-8', newline='') as out:
            
            reader = csv.reader(src, delimiter=';')
            writer = csv.writer(out, delimiter=';')
            
            header = next(reader)
            writer.writerow(header)
            
            id_idx = header.index('ID')
            text_idx = header.index('OriginalText')
            
            for row in reader:
                if len(row) <= max(id_idx, text_idx):
                    writer.writerow(row)
                    continue
                
                total += 1
                id_val = row[id_idx].strip()
                
                if id_val in translations:
                    row[text_idx] = translations[id_val]
                    replaced += 1
                
                writer.writerow(row)
        
        log(f"✅ Применено переводов: {replaced} из {total}")
        return True
    except Exception as e:
        log(f"❌ Ошибка применения: {e}")
        import traceback
        traceback.print_exc()
        return False


def pak_text(csv_path, extract_dir):
    try:
        with open(csv_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f, delimiter=';')
            header = next(reader)
            
            id_idx = header.index('ID')
            file_idx = header.index('File')
            text_idx = header.index('OriginalText')
            all_blocks_idx = header.index('All Blocks')
            work_blocks_idx = header.index('Work Blocks')
            unknown_idx = header.index('Unknown')
            
            base_name = ''
            start_unk = 0
            start_id = 0
            curr_text = 0
            all_blocks = b''
            work_blocks = b''
            file_bytes = b'\xDC\x96\x58\x59\x00\x00\x00\x00'
            filled_bytes_unk = b''
            filled_bytes_id = b''
            filled_bytes_text = b''
            
            for row in reader:
                if row[0] == 'Number' or row[0] == '':
                    continue
                
                file_name = row[file_idx]
                
                if file_name.endswith('_0.dat'):
                    log(f"⏭️  Пропущена строка: {file_name}")
                    continue
                
                if file_name != base_name:
                    if base_name != '':
                        output_path = os.path.join(extract_dir, base_name)
                        with open(output_path, 'wb') as outf:
                            outf.write(all_blocks)
                            outf.write(work_blocks)
                            outf.write(file_bytes)
                            outf.write(filled_bytes_unk)
                            outf.write(filled_bytes_id)
                            outf.write(filled_bytes_text)
                    
                    base_name = str(file_name)
                    all_blocks = struct.pack('<II', int(row[all_blocks_idx]), 0)
                    work_blocks = struct.pack('<II', int(row[work_blocks_idx]), 0)
                    file_bytes = b'\xDC\x96\x58\x59\x00\x00\x00\x00'
                    filled_bytes_unk = b''
                    filled_bytes_id = b''
                    filled_bytes_text = b''
                    
                    start_unk = len(all_blocks) + len(work_blocks) + len(file_bytes)
                    start_id = start_unk + int(row[all_blocks_idx]) + 17
                    curr_text = start_id + int(row[all_blocks_idx]) * 16
                
                text = row[text_idx].replace('\\n', '\x0A').encode('utf-8')
                
                unk_byte = bytes.fromhex(row[unknown_idx])
                filled_bytes_unk += unk_byte
                start_unk += 1
                
                if start_unk >= int(row[all_blocks_idx]) + 24:
                    if len(filled_bytes_unk) >= 16:
                        filled_bytes_unk += b'\xFF' + filled_bytes_unk[:16]
                    else:
                        filled_bytes_unk += b'\xFF' + filled_bytes_unk + b'\x80' * (16 - len(filled_bytes_unk))
                
                id_byte = bytes.fromhex(row[id_idx])
                filled_bytes_id += id_byte
                start_id += 8
                
                offset_len = struct.pack('<II', (curr_text - start_id), len(text))
                filled_bytes_id += offset_len
                start_id += 8
                
                filled_bytes_text += text
                curr_text += len(text)
            
            if base_name != '':
                output_path = os.path.join(extract_dir, base_name)
                with open(output_path, 'wb') as outf:
                    outf.write(all_blocks)
                    outf.write(work_blocks)
                    outf.write(file_bytes)
                    outf.write(filled_bytes_unk)
                    outf.write(filled_bytes_id)
                    outf.write(filled_bytes_text)
        
        return True
    except Exception as e:
        log(f"❌ Ошибка запеканья: {e}")
        import traceback
        traceback.print_exc()
        return False


def pak_file(dat_folder, output_file):
    try:
        files = [f for f in os.listdir(dat_folder) if f.endswith('.dat')]
        
        def extract_number(filename):
            match = re.search(r'(\d+)\.dat$', filename)
            return int(match.group(1)) if match else float('inf')
        
        files.sort(key=extract_number)
        
        log(f"🔍 Найдено файлов для упаковки: {len(files)}")
        
        with open(output_file, 'wb') as outfile:
            # 1. Заголовок + версия
            outfile.write(b'\xEF\xBE\xAD\xDE\x01\x00\x00\x00')
            
            outfile.write(struct.pack('<I', len(files) - 1))
            
            # Собираем архив в памяти
            archive = b''
            for filename in files:
                file_path = os.path.join(dat_folder, filename)
                
                with open(file_path, 'rb') as infile:
                    data = infile.read()
                
                # Пишем смещение ТЕКУЩЕГО файла (кроме последнего)
                if filename != files[-1]:
                    outfile.write(struct.pack('<I', len(archive)))
                
                # Сжимаем и добавляем в архив
                comp_data = pyzstd.compress(data)
                header = struct.pack('<BII', 4, len(comp_data), len(data))
                archive += header + comp_data
            
            outfile.write(struct.pack('<I', len(archive)))
            
            outfile.write(archive)
        
        log(f"✅ Сборка завершена. Упаковано: {len(files)} блоков")
        log(f"✅ Размер архива: {len(archive)} байт")
        log(f"✅ Файл сохранен как: {output_file}")
        return True
    except Exception as e:
        log(f"❌ Ошибка финальной упаковки: {e}")
        import traceback
        traceback.print_exc()
        return False


def process_game_file(input_file, translation_file, work_dir, output_dir):
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    
    log(f"\n{'='*50}")
    log(f"Обработка файла: {base_name}")
    log(f"{'='*50}")
    
    log(f"\n[Распаковка] {base_name}...")
    extract_dir = os.path.join(work_dir, base_name)
    if not extract_file(input_file, work_dir):
        return False
    
    log(f"\n[Извлечение] Текстов из {base_name}...")
    csv_path = extract_text(extract_dir, work_dir, base_name)
    if not csv_path:
        return False
    
    log(f"\n[Перевод] Применяю перевод к {base_name}...")
    translated_csv = os.path.join(work_dir, f"TextExtractor_{base_name}_translated.csv")
    if not apply_translation(translation_file, csv_path, translated_csv):
        return False
    
    log(f"\n[Запеканье] Текстов для {base_name}...")
    if not pak_text(translated_csv, extract_dir):
        return False
    
    log(f"\n[Упаковка] Финальная упаковка {base_name}...")
    output_file = os.path.join(output_dir, f"{base_name}")
    if not pak_file(extract_dir, output_file):
        return False
    
    log(f"\n✅ {base_name} готов!")
    return True


def main():
    parser = argparse.ArgumentParser(description='WWM Translation Builder - Multi-file Pipeline')
    parser.add_argument('--input', '-i', nargs='+', required=True, 
                       help='Входные файлы игры (можно несколько: file1 file2)')
    parser.add_argument('--translation', '-t', required=True, help='TSV перевод (ID\\tTranslation)')
    parser.add_argument('--output', '-o', default='release/', help='Выходная папка для релиза (.bin файлы)')
    parser.add_argument('--workdir', '-w', default='work/', help='Рабочая папка (временные файлы)')
    
    args = parser.parse_args()
    
    os.makedirs(args.output, exist_ok=True)
    os.makedirs(args.workdir, exist_ok=True)
    
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log("📦 WWM Translation Builder (Multi-file Pipeline)")
    log(f"📁 Файлы для обработки: {len(args.input)}")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    for input_file in args.input:
        if not os.path.exists(input_file):
            log(f"❌ Файл не найден: {input_file}")
            return 1
    
    if not os.path.exists(args.translation):
        log(f"❌ Файл перевода не найден: {args.translation}")
        return 1
    
    failed_files = []
    for input_file in args.input:
        if not process_game_file(input_file, args.translation, args.workdir, args.output):
            failed_files.append(input_file)
    
    log("\n" + "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log("📊 ИТОГОВЫЙ ОТЧЁТ")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    success_count = len(args.input) - len(failed_files)
    log(f"✅ Успешно обработано: {success_count}/{len(args.input)}")
    
    if failed_files:
        log(f"❌ Ошибки при обработке:")
        for f in failed_files:
            log(f"   - {f}")
        return 1
    
    log(f"\n📁 Готовые .bin файлы находятся в: {args.output}")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return 0


if __name__ == '__main__':
    sys.exit(main())
