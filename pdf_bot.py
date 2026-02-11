import os
import logging
import hashlib
import json
import requests
import re
import asyncio
import aiohttp
import traceback
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

import ollama
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, CallbackContext, CallbackQueryHandler,
    ContextTypes
)
import PyPDF2
import pdfplumber

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('pdf_assistant.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация
PDF_FOLDER = Path("pdf_documents")
PDF_FOLDER.mkdir(exist_ok=True)
OLLAMA_MODEL = "qwen2.5:14b-instruct-q4_K_M"
CACHE_FILE = "documents_cache.json"
OLLAMA_TIMEOUT = 60  # Увеличиваем таймаут для Ollama
TELEGRAM_TIMEOUT = 30  # Таймаут для Telegram
INTERNET_TIMEOUT = 10  # Таймаут для интернет-запросов

# Глобальный пул потоков для тяжелых операций
executor = ThreadPoolExecutor(max_workers=4)


class AdvancedPDFProcessor:
    """Продвинутый обработчик PDF с кэшированием и семантическим поиском"""

    def __init__(self):
        self.documents_cache: Dict[str, Dict] = {}
        self.chunk_index: Dict[str, List[Dict]] = {}
        self.load_cache()
        self.update_documents()

    def load_cache(self):
        """Загружаем кэш документов"""
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    self.documents_cache = cache_data.get('documents', {})
                    self.chunk_index = cache_data.get('chunks', {})
                logger.info(f"Загружен кэш: {len(self.documents_cache)} документов")
            except Exception as e:
                logger.error(f"Ошибка загрузки кэша: {e}")
                self.documents_cache = {}
                self.chunk_index = {}

    def save_cache(self):
        """Сохраняем кэш"""
        try:
            cache_data = {
                'documents': self.documents_cache,
                'chunks': self.chunk_index,
                'updated_at': datetime.now().isoformat()
            }
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Кэш сохранен в {CACHE_FILE}")
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша: {e}")

    def calculate_file_hash(self, file_path: Path) -> str:
        """Хеш файла для отслеживания изменений"""
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()

    def extract_text_advanced(self, file_path: Path) -> Tuple[str, Dict]:
        """Улучшенное извлечение текста с сохранением структуры"""
        text = ""
        metadata = {
            "pages": 0,
            "sections": [],
            "tables_found": 0,
            "images_found": 0,
            "extraction_method": "unknown"
        }

        try:
            # Метод 1: pdfplumber (лучший для структурированных PDF)
            with pdfplumber.open(file_path) as pdf:
                metadata["pages"] = len(pdf.pages)
                metadata["extraction_method"] = "pdfplumber"

                for i, page in enumerate(pdf.pages, 1):
                    try:
                        # Извлекаем текст без problematical parameters
                        page_text = page.extract_text()
                        if page_text:
                            # Сохраняем структуру документа
                            text += f"\n{'=' * 60}\nСтраница {i}\n{'=' * 60}\n{page_text}\n"

                            # Извлекаем заголовки (строки в верхнем регистре)
                            lines = page_text.split('\n')
                            for line in lines:
                                clean_line = line.strip()
                                if (len(clean_line) > 3 and len(clean_line) < 100 and
                                        clean_line.isupper() and clean_line not in metadata["sections"]):
                                    metadata["sections"].append(clean_line)

                        # Проверяем наличие таблиц
                        try:
                            tables = page.extract_tables()
                            if tables:
                                metadata["tables_found"] += len(tables)
                                text += f"\n[Обнаружено таблиц на странице {i}: {len(tables)}]\n"
                        except Exception as e:
                            logger.debug(f"Ошибка извлечения таблиц: {e}")

                        # Проверяем наличие изображений
                        if page.images:
                            metadata["images_found"] += len(page.images)
                            text += f"\n[Обнаружено изображений на странице {i}: {len(page.images)}]\n"

                    except Exception as e:
                        logger.warning(f"Ошибка обработки страницы {i}: {e}")
                        continue

                return text, metadata

        except Exception as e:
            logger.warning(f"pdfplumber error: {e}")
            try:
                # Метод 2: PyPDF2 (резервный)
                with open(file_path, 'rb') as file:
                    reader = PyPDF2.PdfReader(file)
                    metadata["pages"] = len(reader.pages)
                    metadata["extraction_method"] = "pypdf2"

                    for i, page in enumerate(reader.pages, 1):
                        page_text = page.extract_text()
                        if page_text:
                            text += f"\n{'=' * 60}\nСтраница {i}\n{'=' * 60}\n{page_text}\n"

                    return text, metadata

            except Exception as e2:
                logger.error(f"PyPDF2 error: {e2}")
                return "", metadata

    def chunk_text_intelligently(self, text: str, filename: str) -> List[Dict]:
        """Интеллектуальное разбиение текста на чанки с семантическим группированием"""
        if not text:
            return []

        chunks = []

        # Разбиваем по страницам (если есть маркеры страниц)
        page_markers = re.split(r'\n={10,}\nСтраница \d+\n={10,}\n', text)

        if len(page_markers) > 1:
            # Используем разбиение по страницам
            for i, page_text in enumerate(page_markers[1:], 1):
                if page_text.strip():
                    # Разбиваем страницу на абзацы
                    paragraphs = re.split(r'\n\s*\n', page_text)
                    current_chunk = ""

                    for para in paragraphs:
                        if len(current_chunk) + len(para) < 1500:
                            current_chunk += para + "\n\n"
                        else:
                            if current_chunk.strip():
                                chunks.append({
                                    "text": current_chunk.strip(),
                                    "page": i,
                                    "source": filename,
                                    "chunk_type": "page_section"
                                })
                            current_chunk = para + "\n\n"

                    if current_chunk.strip():
                        chunks.append({
                            "text": current_chunk.strip(),
                            "page": i,
                            "source": filename,
                            "chunk_type": "page_section"
                        })
        else:
            # Разбиваем на смысловые блоки
            sentences = re.split(r'(?<=[.!?])\s+', text)
            current_chunk = ""

            for sentence in sentences:
                if len(current_chunk) + len(sentence) < 1000:
                    current_chunk += sentence + " "
                else:
                    if current_chunk.strip():
                        chunks.append({
                            "text": current_chunk.strip(),
                            "page": 0,
                            "source": filename,
                            "chunk_type": "semantic"
                        })
                    current_chunk = sentence + " "

            if current_chunk.strip():
                chunks.append({
                    "text": current_chunk.strip(),
                    "page": 0,
                    "source": filename,
                    "chunk_type": "semantic"
                })

        # Извлекаем ключевые слова для каждого чанка
        for chunk in chunks:
            chunk["keywords"] = self.extract_keywords(chunk["text"])

        return chunks

    def extract_keywords(self, text: str, max_keywords: int = 10) -> List[str]:
        """Извлекаем ключевые слова из текста"""
        # Убираем служебные слова
        stop_words = {
            'и', 'в', 'на', 'с', 'по', 'для', 'от', 'до', 'из', 'не',
            'что', 'это', 'как', 'так', 'или', 'но', 'за', 'же', 'бы',
            'the', 'and', 'of', 'to', 'in', 'a', 'is', 'that', 'for',
            'iso', 'гост', 'стандарт', 'документ', 'страница'
        }

        # Находим слова (русские и английские)
        words = re.findall(r'\b[a-zA-Zа-яА-ЯёЁ]{3,}\b', text.lower())

        # Считаем частоту
        from collections import Counter
        word_counts = Counter(words)

        # Фильтруем служебные слова и выбираем наиболее частые
        keywords = []
        for word, count in word_counts.most_common(20):
            if word not in stop_words and len(word) > 2:
                keywords.append(f"{word}:{count}")
                if len(keywords) >= max_keywords:
                    break

        return keywords

    def update_documents(self):
        """Обновляем документы с интеллектуальной обработкой"""
        if not PDF_FOLDER.exists():
            PDF_FOLDER.mkdir()
            print(f"📁 Создана папка: {PDF_FOLDER}")
            return

        pdf_files = list(PDF_FOLDER.glob("*.pdf"))
        print(f"📁 Найдено PDF файлов: {len(pdf_files)}")

        updated_count = 0
        self.chunk_index.clear()

        for pdf_file in pdf_files:
            try:
                file_hash = self.calculate_file_hash(pdf_file)
                filename = pdf_file.name

                # Проверяем, нужно ли обновлять
                if filename in self.documents_cache:
                    cached_hash = self.documents_cache[filename].get("file_hash", "")
                    if cached_hash == file_hash:
                        # Восстанавливаем чанки из кэша
                        if filename in self.chunk_index:
                            continue

                # Обрабатываем новый/измененный файл
                print(f"📄 Обрабатываю: {filename}")
                text, metadata = self.extract_text_advanced(pdf_file)

                if text and len(text.strip()) > 100:
                    # Создаем чанки
                    chunks = self.chunk_text_intelligently(text, filename)

                    # Сохраняем в кэш
                    self.documents_cache[filename] = {
                        "file_hash": file_hash,
                        "metadata": metadata,
                        "text_preview": text[:1000],
                        "chunk_count": len(chunks),
                        "processed_at": datetime.now().isoformat(),
                        "file_size": pdf_file.stat().st_size
                    }

                    # Индексируем чанки
                    self.chunk_index[filename] = chunks

                    updated_count += 1
                    print(f"✅ Обработан: {filename} ({metadata['pages']} стр., {len(chunks)} чанков)")
                else:
                    print(f"⚠️ Пустой текст в файле: {filename}")

            except Exception as e:
                print(f"❌ Ошибка обработки {pdf_file}: {e}")
                logger.error(f"Ошибка обработки {pdf_file}: {e}")

        if updated_count > 0:
            self.save_cache()
            print(f"🔄 Обновлено документов: {updated_count}")

        print(f"📚 Всего в кэше: {len(self.documents_cache)} документов")
        total_chunks = sum(len(chunks) for chunks in self.chunk_index.values())
        print(f"🧩 Всего чанков: {total_chunks}")

    def search_with_semantic(self, question: str, max_results: int = 5) -> List[Dict]:
        """Семантический поиск по чанкам"""
        question_lower = question.lower()
        question_words = set(re.findall(r'\b[a-zA-Zа-яА-ЯёЁ]{3,}\b', question_lower))

        results = []

        for filename, chunks in self.chunk_index.items():
            for chunk in chunks:
                chunk_text = chunk["text"].lower()
                chunk_keywords = chunk.get("keywords", [])

                # Вычисляем релевантность
                score = 0

                # 1. Поиск точных совпадений слов
                for word in question_words:
                    if word in chunk_text:
                        score += 2

                # 2. Поиск по ключевым словам чанка
                for kw_entry in chunk_keywords:
                    kw = kw_entry.split(':')[0]
                    if kw in question_words:
                        score += 3

                # 3. Поиск по названию документа
                if any(word in filename.lower() for word in question_words):
                    score += 5

                # 4. Поиск по номеру ГОСТ/ISO
                doc_standard = self.extract_standard_number(filename)
                if doc_standard and doc_standard in question:
                    score += 10

                if score > 0:
                    results.append({
                        "score": score,
                        "text": chunk["text"],
                        "source": filename,
                        "page": chunk.get("page", 0),
                        "chunk_type": chunk.get("chunk_type", "unknown")
                    })

        # Сортируем по релевантности
        results.sort(key=lambda x: x["score"], reverse=True)

        # Убираем дубликаты (похожий текст)
        unique_results = []
        seen_texts = set()

        for result in results[:max_results * 2]:  # Берем больше для фильтрации
            text_hash = hashlib.md5(result["text"][:200].encode()).hexdigest()
            if text_hash not in seen_texts:
                seen_texts.add(text_hash)
                unique_results.append(result)
                if len(unique_results) >= max_results:
                    break

        return unique_results

    def extract_standard_number(self, filename: str) -> Optional[str]:
        """Извлекаем номер стандарта из названия файла"""
        patterns = [
            r'(ГОСТ\s*[0-9.-]+)',
            r'(ISO\s*[0-9.-]+)',
            r'(СТ\s*[0-9.-]+)',
            r'(EN\s*[0-9.-]+)',
            r'([0-9.-]+\s*ГОСТ)',
            r'([0-9.-]+\s*ISO)'
        ]

        for pattern in patterns:
            match = re.search(pattern, filename, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    async def search_internet_fallback(self, question: str) -> Optional[str]:
        """Поиск в интернете как запасной вариант (использует DuckDuckGo)"""
        try:
            # Используем DuckDuckGo Instant Answer API
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=INTERNET_TIMEOUT)) as session:
                url = f"https://api.duckduckgo.com/"
                params = {
                    'q': question,
                    'format': 'json',
                    'no_html': '1',
                    'skip_disambig': '1'
                }

                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        if data.get('AbstractText'):
                            return data['AbstractText']
                        elif data.get('RelatedTopics'):
                            first_topic = data['RelatedTopics'][0]
                            if isinstance(first_topic, dict) and 'Text' in first_topic:
                                return first_topic['Text'][:500]
                            elif isinstance(first_topic, str):
                                return first_topic[:500]

            return None

        except asyncio.TimeoutError:
            logger.warning(f"Таймаут при поиске в интернете: {question}")
            return None
        except Exception as e:
            logger.error(f"Ошибка поиска в интернете: {e}")
            return None


class SmartPDFAssistant:
    """Умный ассистент с улучшенной обработкой PDF и доступом к интернету"""

    def __init__(self, token: str):
        self.token = token
        self.processor = AdvancedPDFProcessor()
        self.application = None

    def check_ollama(self) -> bool:
        """Проверяем подключение к Ollama"""
        try:
            response = requests.get("http://localhost:11434/api/tags", timeout=10)

            if response.status_code == 200:
                data = response.json()
                models = data.get('models', [])

                model_names = []
                for model in models:
                    if 'name' in model:
                        model_names.append(model['name'])
                    elif 'model' in model:
                        model_names.append(model['model'])

                print(f"🤖 Доступные модели: {', '.join(model_names)}")

                for name in model_names:
                    if OLLAMA_MODEL in name:
                        print(f"✅ Модель {OLLAMA_MODEL} найдена")
                        return True

                print(f"❌ Модель {OLLAMA_MODEL} не найдена")
                return False
            else:
                print(f"❌ Ошибка HTTP: {response.status_code}")
                return False

        except Exception as e:
            print(f"❌ Ошибка подключения к Ollama: {e}")
            return False

    async def ask_ollama_with_timeout(self, messages: List[Dict], timeout: int = OLLAMA_TIMEOUT) -> Dict:
        """Запрос к Ollama с таймаутом"""
        try:
            loop = asyncio.get_event_loop()
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    executor,
                    lambda: ollama.chat(
                        model=OLLAMA_MODEL,
                        messages=messages,
                        options={
                            'temperature': 0.3,
                            'num_predict': 1200,
                            'num_thread': 4  # Увеличиваем производительность
                        }
                    )
                ),
                timeout=timeout
            )
            return response
        except asyncio.TimeoutError:
            logger.error(f"Таймаут при запросе к Ollama (>{timeout} сек)")
            raise
        except Exception as e:
            logger.error(f"Ошибка запроса к Ollama: {e}")
            raise

    async def ask_question_with_fallback(self, question: str) -> Tuple[str, str, bool]:
        """Задаем вопрос с fallback на интернет"""
        # Поиск в документах
        search_results = self.processor.search_with_semantic(question)

        if search_results:
            # Формируем контекст из документов
            context = "ИНФОРМАЦИЯ ИЗ ДОКУМЕНТОВ:\n\n"
            sources = set()

            for i, result in enumerate(search_results, 1):
                context += f"Источник {i}: {result['source']} (релевантность: {result['score']})\n"
                context += f"Текст:\n{result['text'][:800]}...\n\n"
                sources.add(result['source'])

            sources_text = ", ".join(sources)

            prompt = f"""Ты - технический эксперт. Отвечай на основе предоставленных документов.

{context}

ВОПРОС: {question}

ИНСТРУКЦИИ:
1. Ответь на основе предоставленных документов
2. Будь точным и конкретным
3. Упоминай источники информации
4. Если информации недостаточно, добавь свои знания

ОТВЕТ:"""

            try:
                response = await self.ask_ollama_with_timeout(
                    messages=[
                        {"role": "system",
                         "content": "Ты - технический эксперт, который предоставляет точную информацию."},
                        {"role": "user", "content": prompt}
                    ]
                )
                answer = response['message']['content']
                answer += f"\n\n📚 *Источники:* {sources_text}"
                return answer, "documents", True

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут при обработке вопроса с документами: {question}")
                # Пробуем более простой запрос без контекста
                try:
                    simple_response = await self.ask_ollama_with_timeout(
                        messages=[
                            {"role": "system", "content": "Ты - технический эксперт."},
                            {"role": "user", "content": question}
                        ],
                        timeout=15
                    )
                    answer = simple_response['message']['content']
                    answer += f"\n\n📚 *Источники:* {sources_text}\n⚠️ *Примечание:* Ответ сгенерирован без глубокого анализа документов из-за таймаута"
                    return answer, "documents_timeout", True
                except:
                    # Если и это не сработало, ищем в интернете
                    pass

            except Exception as e:
                logger.error(f"Ошибка Ollama: {e}")

        # Документов не найдено или ошибка - ищем в интернете
        print("🔍 Информация не найдена в документах, ищу в интернете...")

        internet_info = await self.processor.search_internet_fallback(question)

        if internet_info:
            prompt = f"""Ты - технический эксперт. Ответь на вопрос, используя предоставленную информацию из интернета.

ИНФОРМАЦИЯ ИЗ ИНТЕРНЕТА:
{internet_info}

ВОПРОС: {question}

ИНСТРУКЦИИ:
1. Ответь на основе предоставленной информации
2. Будь точным и конкретным
3. Упомяни, что информация из внешних источников

ОТВЕТ:"""

            try:
                response = await self.ask_ollama_with_timeout(
                    messages=[
                        {"role": "system", "content": "Ты - технический эксперт."},
                        {"role": "user", "content": prompt}
                    ],
                    timeout=15
                )
                answer = response['message']['content']
                answer += "\n\n⚠️ *Примечание:* Информация взята из открытых источников в интернете"
                return answer, "internet", True

            except Exception as e:
                logger.error(f"Ошибка Ollama при обработке интернет-информации: {e}")
                internet_fallback = f"Информация из интернета:\n{internet_info}"
                return internet_fallback, "internet_raw", True

        else:
            # Ничего не найдено
            return "❌ К сожалению, не удалось найти информацию ни в документах, ни в интернете. Попробуйте уточнить вопрос.", "not_found", False

    async def start(self, update: Update, context: CallbackContext):
        """Команда /start"""
        doc_count = len(self.processor.documents_cache)
        chunk_count = sum(len(chunks) for chunks in self.processor.chunk_index.values())

        welcome_text = f"""
🤖 *Умный PDF Assistant*

📚 *Документов:* {doc_count}
🧩 *Чанков:* {chunk_count}
🧠 *Модель:* {OLLAMA_MODEL}

*Возможности:*
1. Интеллектуальный поиск по документам
2. Fallback на интернет при отсутствии информации
3. Семантический анализ PDF
4. Указание источников информации

*Команды:*
/start - это сообщение
/docs - список документов
/reload - обновить документы
/status - статистика

*Примеры вопросов:*
• Что такое ISO 12944?
• Какие ГОСТы по покраске металла?
• Требования к подготовке поверхности
• Объясни стандарт ГОСТ 9.402-2004
"""

        keyboard = [
            [InlineKeyboardButton("📚 Документы", callback_data='list_docs')],
            [InlineKeyboardButton("🔄 Обновить", callback_data='reload_docs')],
            [InlineKeyboardButton("📊 Статистика", callback_data='status')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.message:
            await update.message.reply_text(welcome_text, parse_mode='Markdown', reply_markup=reply_markup)
        elif update.callback_query:
            await update.callback_query.message.reply_text(welcome_text, parse_mode='Markdown',
                                                           reply_markup=reply_markup)

    async def show_documents(self, update: Update, context: CallbackContext):
        """Показать список документов"""
        # Обрабатываем callback_query если есть
        query = update.callback_query
        if query:
            await query.answer()
            chat_id = query.message.chat_id
            message_id = query.message.message_id
        else:
            # Это команда из сообщения
            chat_id = update.effective_chat.id
            message_id = None

        if not self.processor.documents_cache:
            message_text = "📭 В папке нет PDF-файлов.\n" \
                           f"Добавьте файлы в папку `{PDF_FOLDER}`"

            if query:
                await query.edit_message_text(message_text, parse_mode='Markdown')
            else:
                await context.bot.send_message(chat_id, message_text, parse_mode='Markdown')
            return

        doc_list = "📚 *Загруженные документы:*\n\n"
        for filename, doc_data in self.processor.documents_cache.items():
            metadata = doc_data.get("metadata", {})
            pages = metadata.get("pages", 0)
            chunks = doc_data.get("chunk_count", 0)
            method = metadata.get("extraction_method", "unknown")

            doc_list += f"📄 *{filename}*\n"
            doc_list += f"   Страниц: {pages} | Чанков: {chunks}\n"
            doc_list += f"   Метод: {method}\n\n"

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Обновить", callback_data='reload_docs')
        ]])

        if query:
            await query.edit_message_text(doc_list, parse_mode='Markdown', reply_markup=keyboard)
        else:
            await context.bot.send_message(chat_id, doc_list, parse_mode='Markdown', reply_markup=keyboard)

    async def reload_documents(self, update: Update, context: CallbackContext):
        """Обновить документы"""
        # Обрабатываем callback_query если есть
        query = update.callback_query
        if query:
            await query.answer("Обновляю документы...")
            chat_id = query.message.chat_id
            message_id = query.message.message_id
            edit_message = True
        else:
            chat_id = update.effective_chat.id
            message_id = None
            edit_message = False

        # Отправляем сообщение о начале обновления
        if edit_message:
            message = await query.edit_message_text("🔄 Обновляю документы... Это может занять несколько минут.")
        else:
            message = await context.bot.send_message(chat_id,
                                                     "🔄 Обновляю документы... Это может занять несколько минут.")

        # Запускаем обновление в отдельном потоке
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                executor,
                self.processor.update_documents
            )

            doc_count = len(self.processor.documents_cache)
            message_text = f"✅ Документы обновлены!\nЗагружено: {doc_count} документов"

            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("📚 Список", callback_data='list_docs')
            ]])

            if edit_message:
                await query.edit_message_text(message_text, reply_markup=keyboard)
            else:
                await message.edit_text(message_text, reply_markup=keyboard)

        except Exception as e:
            error_msg = f"❌ Ошибка при обновлении документов: {str(e)[:100]}"
            if edit_message:
                await query.edit_message_text(error_msg)
            else:
                await message.edit_text(error_msg)

    async def handle_message(self, update: Update, context: CallbackContext):
        """Обработка вопросов"""
        question = update.message.text.strip()

        if not question:
            await update.message.reply_text("Пожалуйста, введите вопрос.")
            return

        # Показываем "печатает..."
        await context.bot.send_chat_action(
            chat_id=update.effective_chat.id,
            action="typing"
        )

        try:
            # Получаем ответ с fallback
            answer, source_type, success = await self.ask_question_with_fallback(question)

            # Форматируем ответ
            response = f"❓ *Вопрос:* {question}\n\n"
            response += f"🤖 *Ответ:*\n{answer}\n\n"

            if source_type == "documents":
                response += "📚 *Источник:* Документы из папки"
            elif source_type == "documents_timeout":
                response += "📚⏱️ *Источник:* Документы (обработка с таймаутом)"
            elif source_type == "internet":
                response += "🌐 *Источник:* Интернет (открытые источники)"
            elif source_type == "internet_raw":
                response += "🌐 *Источник:* Необработанные данные из интернета"
            else:
                response += "⚠️ *Источник:* Информация не найдена"

            # Отправляем ответ частями если он слишком длинный
            if len(response) > 4000:
                parts = [response[i:i + 4000] for i in range(0, len(response), 4000)]
                for part in parts:
                    await update.message.reply_text(part, parse_mode='Markdown')
                    await asyncio.sleep(0.5)
            else:
                await update.message.reply_text(response, parse_mode='Markdown')

        except asyncio.TimeoutError:
            logger.error(f"Таймаут обработки вопроса: {question}")
            await update.message.reply_text(
                "⏱️ *Таймаут обработки*\n"
                "Запрос занял слишком много времени. Попробуйте:\n"
                "1. Переформулировать вопрос\n"
                "2. Задать более конкретный вопрос\n"
                "3. Проверить доступность модели Ollama",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Ошибка обработки вопроса: {e}\n{traceback.format_exc()}")
            await update.message.reply_text(f"❌ Ошибка: {str(e)[:200]}")

    async def show_status(self, update: Update, context: CallbackContext):
        """Показать статус"""
        # Обрабатываем callback_query если есть
        query = update.callback_query
        if query:
            await query.answer()
            chat_id = query.message.chat_id
            message_id = query.message.message_id
            edit_message = True
        else:
            chat_id = update.effective_chat.id
            message_id = None
            edit_message = False

        doc_count = len(self.processor.documents_cache)
        chunk_count = sum(len(chunks) for chunks in self.processor.chunk_index.values())
        total_size = sum(doc.get("file_size", 0) for doc in self.processor.documents_cache.values())

        status_text = f"""
📊 *Статус системы:*

🤖 *Модель:* {OLLAMA_MODEL}
📚 *Документов:* {doc_count}
🧩 *Чанков:* {chunk_count}
💾 *Общий размер:* {total_size / 1024 / 1024:.1f} MB
⏱️ *Таймаут Ollama:* {OLLAMA_TIMEOUT} сек

📁 *Папка с документами:* `{PDF_FOLDER}`
"""

        if edit_message:
            await query.edit_message_text(status_text, parse_mode='Markdown')
        else:
            await context.bot.send_message(chat_id, status_text, parse_mode='Markdown')

    async def button_callback(self, update: Update, context: CallbackContext):
        """Обработчик кнопок"""
        query = update.callback_query
        await query.answer()

        if query.data == 'list_docs':
            await self.show_documents(update, context)
        elif query.data == 'reload_docs':
            await self.reload_documents(update, context)
        elif query.data == 'status':
            await self.show_status(update, context)

    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик ошибок"""
        logger.error(f"Ошибка: {context.error}")

        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"⚠️ Произошла ошибка: {str(context.error)[:200]}"
                )
            except:
                pass

    def run(self):
        """Запуск бота"""
        # Проверяем Ollama
        print("🔍 Проверяем подключение к Ollama...")
        if not self.check_ollama():
            print("❌ Ollama недоступен. Запустите: ollama serve")
            print(f"ℹ️ Убедитесь, что модель загружена: ollama pull {OLLAMA_MODEL}")
            return

        print("✅ Ollama доступен!")
        print(f"📁 Загружено документов: {len(self.processor.documents_cache)}")

        # Создаем приложение с увеличенными таймаутами
        application = Application.builder() \
            .token(self.token) \
            .read_timeout(TELEGRAM_TIMEOUT) \
            .write_timeout(TELEGRAM_TIMEOUT) \
            .connect_timeout(TELEGRAM_TIMEOUT) \
            .pool_timeout(TELEGRAM_TIMEOUT) \
            .build()

        self.application = application

        # Регистрируем обработчики
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("docs", self.show_documents))
        application.add_handler(CommandHandler("reload", self.reload_documents))
        application.add_handler(CommandHandler("status", self.show_status))
        application.add_handler(CallbackQueryHandler(self.button_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

        # Регистрируем обработчик ошибок
        application.add_error_handler(self.error_handler)

        # Запускаем
        logger.info("🤖 Бот запущен...")
        print("\n" + "=" * 60)
        print("🚀 Умный PDF Assistant запущен!")
        print(f"📁 Папка: {PDF_FOLDER}")
        print(f"🧠 Модель: {OLLAMA_MODEL}")
        print(f"📚 Документов: {len(self.processor.documents_cache)}")
        print(f"⏱️ Таймаут Ollama: {OLLAMA_TIMEOUT} сек")
        print("🌐 Интернет: ДОСТУПЕН")
        print("=" * 60)
        print("\nНажмите Ctrl+C для остановки.\n")

        # Запускаем с обработкой исключений
        try:
            application.run_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True
            )
        except KeyboardInterrupt:
            print("\n\n🛑 Бот остановлен пользователем")
        except Exception as e:
            logger.error(f"Критическая ошибка при запуске бота: {e}")
            print(f"❌ Критическая ошибка: {e}")


def main():
    """Основная функция"""
    # Получаем токен
    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        print("🤖 Введите токен Telegram бота от @BotFather:")
        token = input("Token: ").strip()
        if not token:
            print("❌ Токен не указан!")
            return
        os.environ["TELEGRAM_TOKEN"] = token

    # Запускаем бота
    bot = SmartPDFAssistant(token)
    bot.run()


if __name__ == "__main__":
    main()