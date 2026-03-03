# -*- coding: utf-8 -*-
"""
AkCent adapter — сборщик по шаблону CS (использует scripts/cs/core.py).
Важно: здесь только "индивидуальная часть" поставщика: скачивание XML и сбор сырья -> OfferOut.
Все правила шаблона (описание/keywords/price/params/валидация) — в cs.core.
"""

from __future__ import annotations

import re
from xml.etree import ElementTree as ET


def _detect_xml_encoding(data: bytes) -> str:
    """Пытаемся вытащить encoding из XML-декларации, иначе utf-8."""
    try:
        head = data[:400].decode("ascii", errors="ignore")
        m = re.search(r'encoding=["\']([^"\']+)["\']', head, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return "utf-8"

def _sanitize_xml_text(s: str) -> str:
    """Чистим мусорные символы/битые амперсанды, чтобы парсер не падал."""
    if not s:
        return s
    # Удаляем управляющие символы (кроме \t \n \r)
    s = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", s)
    # Экранируем голые '&' (не сущности)
    s = re.sub(r"&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9A-Fa-f]+;)", "&amp;", s)
    return s

def _xml_from_bytes_safe(data: bytes):
    """Надёжный парсинг XML: пытаемся как есть, затем с декодированием/очисткой, затем lxml(recover)."""
    # 1) пробуем как есть
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        # 2) декодируем по XML-encoding и чистим
        enc = _detect_xml_encoding(data)
        try:
            text = data.decode(enc, errors="replace")
        except Exception:
            text = data.decode("utf-8", errors="replace")
        text = _sanitize_xml_text(text)
        try:
            return ET.fromstring(text)
        except ET.ParseError:
            # 3) lxml recover (если доступен)
            try:
                from lxml import etree  # type: ignore
                parser = etree.XMLParser(recover=True, huge_tree=True)
                root = etree.fromstring(data, parser=parser)
                return ET.fromstring(etree.tostring(root, encoding="utf-8"))
            except Exception:
                raise e


import requests

from cs.core import (
    OfferOut,
    clean_params,
    compute_price,
    get_public_vendor,
    next_run_at_hour,
    now_almaty,
    safe_int,
    write_cs_feed,
    write_cs_feed_raw,
)

SUPPLIER_NAME = "AkCent"
SUPPLIER_URL = "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"
OUT_FILE = "docs/akcent.yml"
OUTPUT_ENCODING = "utf-8"
SCHEDULE_HOUR_ALMATY = 2
# Версия скрипта (для отладки в GitHub Actions)
BUILD_AKCENT_VERSION = "build_akcent_v48_keystone_warranty_norm"
AKCENT_NAME_PREFIXES: list[str] = [
    "C13T55",
    "Ёмкость для отработанных чернил",
    "Интерактивная доска",
    "Интерактивная панель",
    "Интерактивный дисплей",
    "Картридж",
    "Ламинатор",
    "Монитор",
    "МФУ",
    "Переплетчик",
    "Пленка для ламинирования",
    "Плоттер",
    "Принтер",
    "Проектор",
    "Сканер",
    "Чернила",
    "Шредер",
    "Экономичный набор",
    "Экран",
]

# Префиксы в casefold (для нечувствительности к регистру)
AKCENT_NAME_PREFIXES_CF = tuple((p or "").casefold() for p in AKCENT_NAME_PREFIXES)

# Параметры AkCent, которые не являются характеристиками (только для этого поставщика)
AKCENT_PARAM_DROP = {"Артикул", "Сопутствующие товары"}

# CS: исключаем "картриджи для фильтра/бутылки" Philips AWP (не наша категория)
AKCENT_DROP_ARTICLES = {"AWP201/10", "AWP286/10"}

# Иногда поставщик кладёт страну в vendor/Производитель — такие значения лучше не использовать как бренд

# ---------------------------
# Param schema (AkCent)
# Цель: убираем "мусорные" param-ключи и фиксируем порядок/нормализацию без угадываний.
# Схема собрана по фактическому набору ключей из akcent_r15.yml (147 offers).
AKCENT_SCHEMA_MAX_KEY_LEN = 60
AKCENT_SCHEMA_MAX_KEY_WORDS = 8

AKCENT_SCHEMA_DROP_KEY_EXACT = {
    "EcoTank обеспечивает удобную и недорогую печать в домашних условиях",
    "Комбинированный картридж с голубыми, пурпурными и желтыми чернилами (C13T26704010) для Epson WorkForce WF-100W",
}

AKCENT_SCHEMA_ALLOW_BY_KIND = {
  "shredder": [
    "100% Jam-Proof Технология предотвращения заторов",
    "Auto-Oil™",
    "Energy Savings System",
    "SafeSense® Технология",
    "Safety Lock",
    "SilentShred®",
    "Sleep Mode (автоотключение)",
    "Автоматическая защита от перегрева",
    "Автоотключение при снятой корзине",
    "Автостоп при заполнении корзины",
    "Вес розничной упаковки, кг",
    "Вес транспортной упаковки, кг",
    "Вес, кг",
    "Габаритные размеры, мм (ВxШxГ)",
    "Гарантия",
    "Гарантия на ножи, лет",
    "Гарантия, лет",
    "Датчик пуска при подаче бумаги",
    "Другие технологии предотвращения заторов",
    "Емкость корзины в листах А4, приближенно",
    "Емкость корзины, л",
    "Емкость корзины, литров",
    "Загрузка, листов (A4, 70 гр./м²)",
    "Загрузка, листов (A4, 70гр/м²)",
    "Индикатор возникновения затора",
    "Индикатор наполнения корзины",
    "Индикатор открытой дверцы",
    "Индикатор перегрева",
    "Кабель питания",
    "Количество в транспортной упаковке",
    "Количество пользователей",
    "Количество фрагментов при резке листа А4, шт.",
    "Мощность, Вт",
    "Наличие колес для перемещения",
    "Отдельная корзина для фрагментов CD",
    "Отдельный слот для CD",
    "Производитель",
    "Производительность уничтожителя",
    "Рабочий цикл, мин",
    "Размер резки, мм",
    "Размеры розничной упаковки, мм (ВxШxГ)",
    "Размеры транспортной упаковки, мм (ВxШxГ)",
    "Рекомендуемое количество CD дисков в день",
    "Рекомендуемое количество кредитных карт в день",
    "Рекомендуемое количество листов в день",
    "Селектор количества листов",
    "Сила тока, А",
    "Скорость резки, м/мин",
    "Страна происхождения",
    "Тип",
    "Тип корзины",
    "Тип резки",
    "Тип электродвигателя",
    "Уничтожение",
    "Уничтожение CD или Blu-Ray DVD (1,2 слоя)",
    "Уничтожение кредитных карт",
    "Уничтожение скрепок",
    "Уничтожение степлерных скоб",
    "Уровень секретности",
    "Уровень секретности для CD дисков, DIN 66399",
    "Уровень секретности для кредитных карт, DIN 66399",
    "Уровень секретности, DIN 66399",
    "Уровень шума без загрузки, дБ",
    "Уровень шума под нагрузкой, дБ"
  ],
  "laminator": [
    "Auto Shut Off (Автоотключение)",
    "AutoSense",
    "Easi-Access механизм",
    "HeatGuard™",
    "InstaHeat",
    "Вес розничной упаковки, кг",
    "Вес транспортной упаковки, кг",
    "Вес, кг",
    "Время нагрева, мин",
    "Выходной лоток",
    "Габаритные размеры, мм (ВxШxГ)",
    "Гарантия",
    "Гарантия, лет",
    "Кол-во валов",
    "Количество в транспортной упаковке",
    "Макс. скорость, см/мин",
    "Макс. формат",
    "Максимальная толщина ламинирования (лист+пленка), мм",
    "Освобождение",
    "Персональные настройки",
    "Производитель",
    "Размеры розничной упаковки, мм (ВxШxГ)",
    "Размеры транспортной упаковки, мм (ВxШxГ)",
    "Реверс",
    "Регулировка скорости",
    "Регулировка температуры",
    "Рекомендуемое количество ламинирований в день",
    "Сигнал готовности",
    "Система нагрева",
    "Стартовый набор для 10 документов",
    "Страна происхождения",
    "Съемный сетевой кабель",
    "Тип",
    "Толщина пленки, мкм",
    "Холодное ламинирование"
  ],
  "scanner": [
    "Digital Ice, непрозрачные оригиналы",
    "Digital Ice, пленка",
    "Dual Lens System",
    "Epson ReadyScan LED",
    "QR (20 мил)",
    "Автоподатчик для пленок",
    "Аккумулятор",
    "Вес",
    "Вид",
    "Влажность",
    "Возможности",
    "Время аккумулятора",
    "Время работы зарядки",
    "Гарантия",
    "Глубина цвета, бит",
    "Датчик",
    "Интерполяционное разрешение, dpi",
    "Интерфейс",
    "Интерфейс IEEE-1394 (FireWire)",
    "Интерфейс USB",
    "Максимальный формат сканирования",
    "Материал корпуса",
    "Относительная контрастность",
    "Пленка 203х254 мм",
    "Пленка 35 мм",
    "Пленка, слайды 4\"х5\"",
    "Пленка, слайды 6х12 см",
    "Пленка, слайды 6х20 см",
    "Подключение по Wi-Fi",
    "Подсветка",
    "Поле зрения",
    "Применение",
    "Производитель",
    "Рабочая температура",
    "Рабочий ток",
    "Рабочий ток в режиме ожидания",
    "Размеры (мм)",
    "Разрешение",
    "Разрешение сканера, dpi",
    "Режим контрастности",
    "Режим связи",
    "Сертификаты",
    "Сканирование с планшета",
    "Скорость сканирования",
    "Слайд-модуль",
    "Слайды 35 мм",
    "Страна происхождения",
    "Температура хранения",
    "Тип",
    "Тип датчика",
    "Тип интерфейса",
    "Тип лампы",
    "Тип сканера",
    "Точность распознавания",
    "Уведомление",
    "Устройство",
    "Фокусировка",
    "Форм-фактор",
    "Электростатический разряд"
  ],
  "printer_mfp": [
    "Автоматическая Двусторонняя печать",
    "Автоподатчик",
    "Вид",
    "Гарантия",
    "Гарантия, мес",
    "Двусторонняя печать",
    "ЖК дисплей",
    "Интерфейс",
    "Коды",
    "Количество слотов для картриджей",
    "Количество цветов",
    "Максимальная плотность бумаги, г/м2",
    "Максимальная скорость печати А4 стр/мин",
    "Максимальное разрешение копира",
    "Максимальное разрешение, dpi",
    "Максимальный формат",
    "Минимальная плотность бумаги, г/м2",
    "Минимальный объем капли, пл",
    "Модель",
    "Назначение",
    "Область печати, мм",
    "Область применения",
    "Печать без полей",
    "Печать на CD/DVD",
    "Печать на рулоне",
    "Печать фото",
    "Печать фотографий",
    "Применение",
    "Производитель",
    "Разрешение печати,dpi",
    "Разрешение сканера,dpi",
    "Серия устройств",
    "Скорость печати 10x15 см (цветн.), фото/сек",
    "Скорость печати ISO/IEC 24734 (цветн. А4), стр/мин",
    "Скорость печати ISO/IEC 24734 (ч/б А4), стр/мин",
    "Скорость печати в режиме драфт (цветн. А4), стр/мин",
    "Скорость печати в режиме драфт (ч/б А4), стр/мин",
    "Страна происхождения",
    "Сферы бизнеса",
    "Теxнология печати",
    "Технология печати",
    "Тип",
    "Тип печати",
    "Тип расходных материалов",
    "Тип устройства",
    "Тип чернил",
    "Типовое назначение",
    "Устройство",
    "Факс",
    "Формат",
    "Функционирование",
    "Функция копирования",
    "Цвет корпуса",
    "Цветность"
  ],
  "consumable": [
    "Гарантия",
    "Для бренда",
    "Коды",
    "Количество в упаковке, шт.",
    "Модель",
    "Объем",
    "Объем, мл",
    "Производитель",
    "Ресурс",
    "Совместимость",
    "Страна происхождения",
    "Тип",
    "Тип печати",
    "Цвет"
  ],
  "other": [
    "Adaptive Sync",
    "DisplayPort вход (количество)",
    "HDMI 2.0 вход (количество)",
    "HDMI вход (количество)",
    "HDMI доп информация",
    "VGA (D-Sub)",
    "Активная область",
    "Аудиовыход (3.5 мм)",
    "Вес Нетто, кг",
    "Вес без подставки",
    "Вес брутто",
    "Вес брутто, кг",
    "Вес нетто",
    "Вес, кг",
    "Видимая область экрана, дюймов",
    "Влажность",
    "Внешние Размеры",
    "Внешний Размер",
    "Время отклика",
    "Время отклика (GTG)",
    "Время отклика (MPTR)",
    "Время прикосновения маркера к доске",
    "Встроенные динамики",
    "Встроенный NFC считыватель",
    "Входы (на задней стороне)",
    "Входы (на передней стороне)",
    "Габариты, см",
    "Гарантия",
    "Глубина Упаковки, см",
    "Глубина цвета",
    "Диагональ",
    "Диагональ экрана, дюйм",
    "Диагональ экрана, см",
    "Динамическая контрастность",
    "Дополнительные свойства пленки",
    "Жесты",
    "Звук",
    "Изгиб экрана",
    "Изогнутый экран",
    "Индикатор состояния",
    "Интерфейс подключения",
    "Комбинированный вход/выход микрофон/наушники",
    "Контрастность",
    "Крепление VESA",
    "Метод ввода",
    "Микрофоны",
    "Минимальный предмет касания к доске",
    "Напряжение тока",
    "Основной цвет",
    "Панель управления",
    "Подключение",
    "Подсветка",
    "Покрытие экрана",
    "Производитель",
    "Рабочая",
    "Рабочая температура",
    "Рабочее напряжение",
    "Размер пикселя",
    "Размер упаковки",
    "Размер упаковки, см",
    "Разрешение",
    "Разрешение (Интерполяция)",
    "Разрешение экрана",
    "Разъем для Kensington Lock",
    "Расчетный вес товара с упаковкой, кг",
    "Регулировка наклона (вниз/вверх)",
    "Сертификаты",
    "Сигнальные кабели в комплекте",
    "Скорость отклика",
    "Совместимость",
    "Соотношение сторон",
    "Срок гарантии",
    "Срок службы",
    "Срок службы подсветки",
    "Стилус",
    "Страна происхождения",
    "Температура Эксплуатации",
    "Технологии защиты зрения",
    "Технология",
    "Технология распознавания",
    "Тип",
    "Тип блока питания",
    "Тип гарантии",
    "Тип дисплея",
    "Тип матрицы",
    "Тип пленки",
    "Тип управления",
    "Толщина (мкм)",
    "Углы обзора",
    "Упаковка, шт",
    "Физический размер без подставки, мм",
    "Физический размер с подставкой, мм",
    "Формат",
    "Хранение",
    "Хранения",
    "Цвет",
    "Цвет рамки",
    "Цвета",
    "Цветовой охват NTSC, %",
    "Цветовой охват sRGB, %",
    "Частота обновления (Макс.)",
    "Число касаний",
    "Эксплуатации",
    "Энергопотребление",
    "Энергопотребление, максимальное",
    "Энергопотребление, режим ожидания",
    "Яркость",
    "Яркость (maximum)"
  ],
  "projector": [
    "3D",
    "Ethernet (RJ-45)",
    "HDMI вход (количество)",
    "HDMI доп информация",
    "RS232",
    "USB",
    "USB (Питание внешних устройств)",
    "Wi-Fi",
    "Аппаратное разрешение (проектор)",
    "Аудио",
    "Аудиовход (3.5 мм)",
    "Аудиовыход (3.5 мм)",
    "Безопасность",
    "Варианты проекции",
    "Вентиляционный шум",
    "Вертикальная развёртка",
    "Вес",
    "Вес Нетто, кг",
    "Вид",
    "Видео вход DVI-D",
    "Видео вход DisplayPort",
    "Видео вход HDMI",
    "Видео вход S-video",
    "Видео вход VGA",
    "Время отклика (GTG)",
    "Вручную vertical",
    "Встроенные динамики",
    "Встроенный динамик",
    "Встроенный звук",
    "Габариты товара",
    "Гарантия",
    "Глубина Упаковки, см",
    "Глубина цвета",
    "Диапазон рабочих температур",
    "Динамик",
    "Динамическая контрастность",
    "ЖК дисплей, дюймы",
    "Интерактивный",
    "Интерфейс HDBaseT/DIGITAL LINK",
    "Интерфейсы",
    "Интерфейсы и порты",
    "Используемая технология LCD",
    "Источник света",
    "Категория",
    "Коды",
    "Количество отображаемых цветов",
    "Контрастность",
    "Контрастность изображения проектора",
    "Корр. трапец. искажений вертикальная",
    "Корр. трапец. искажений горизонтальная",
    "Коррекция трапецеидальных искажений",
    "Коррекция трапецеидальных искажений (°)",
    "Коррекция трапеции",
    "Лампа",
    "Максимальная диагональ изображения",
    "Максимальное поддерживаемое разрешение",
    "Максимальное проекционное расстояние",
    "Минимальная диагональ изображения",
    "Минимальное проекционное расстояние",
    "Множество портов",
    "Модель",
    "Мощность лампы",
    "Операционная система",
    "Оптический зум",
    "Оригинальное разрешение",
    "Основной цвет",
    "Память",
    "Поддерживаемое разрешение",
    "Поддерживаемые разрешения",
    "Поддержка форматов изображения",
    "Подключение",
    "Подключения",
    "Проекционная система",
    "Проекционное отношение (макс)",
    "Проекционное отношение (мин)",
    "Проекционное расстояние",
    "Проекционные расстояния",
    "Проекционный коэффициент",
    "Производитель",
    "Работа от аккумуляторных батарей",
    "Рабочая температура",
    "Размер изображения",
    "Размер проекции",
    "Размеры",
    "Разрешение",
    "Разрешение матрицы",
    "Расстояние от проектора до экрана",
    "Расчетный вес товара с упаковкой, кг",
    "Ресурс лампы",
    "Свойства объектива",
    "Сдвиг объектива проектора",
    "Слышимый шум",
    "Смещение",
    "Смещение проекции",
    "Соотношение сторон",
    "Срок гарантии",
    "Срок службы источника света",
    "Срок службы лампы (норм./ эконом.) ч.",
    "Страна производства",
    "Страна происхождения",
    "Технология",
    "Технология проекции",
    "Технология цветопередачи",
    "Тип",
    "Тип DMD",
    "Тип блока питания",
    "Тип гарантии",
    "Тип источника света",
    "Тип лампы",
    "Тип матрицы",
    "Тип проектора",
    "Типы проекции",
    "Уровень шума (норм./эконом.)",
    "Уровень шума (норм./эконом.) Дб",
    "Фокусировка",
    "Функциональные особенности",
    "Характеристики встроенных динамиков",
    "Цвет",
    "Цвета",
    "Цветовая яркость",
    "Цифровой зум",
    "Энергопотребление",
    "Энергопотребление в экономичном режиме",
    "Энергопотребление, режим ожидания",
    "Яркость",
    "Яркость (ANSI LUMEN)",
    "Яркость (ANSI) лмн",
    "Яркость (LED LUMEN)",
    "Яркость, ANSI"
  ],
  "screen": [
    "Вес без упак.",
    "Вес в упак.",
    "Габариты",
    "Гарантия",
    "Диагональ экрана",
    "Корпус",
    "Механизм",
    "Мобильность",
    "Основание",
    "Поверхность",
    "Полотно",
    "Производитель",
    "Размер дм.(м.)",
    "Соотношение сторон",
    "Страна происхождения",
    "Тип",
    "Управление",
    "Формат",
    "Цвет"
  ]
}

AKCENT_SCHEMA_PRIORITY_BY_KIND = {
  "projector": [
    "Производитель",
    "Модель",
    "Тип",
    "Разрешение",
    "Технология",
    "Яркость (ANSI) лмн",
    "Яркость",
    "Цветовая яркость",
    "Контрастность",
    "Проекционный коэффициент",
    "Проекционное расстояние",
    "Соотношение сторон",
    "Источник света",
    "Тип источника света",
    "Срок службы лампы (норм./ эконом.) ч.",
    "Ресурс лампы",
    "Уровень шума (норм./эконом.) Дб",
    "Wi-Fi",
    "Ethernet (RJ-45)",
    "USB",
    "Видео вход HDMI",
    "Видео вход VGA",
    "Видео вход DVI-D",
    "Видео вход DisplayPort",
    "Видео вход S-video",
    "Интерактивный",
    "3D",
    "Вес",
    "Цвет",
    "Страна происхождения",
    "Гарантия",
    "Коды"
  ],
  "screen": [
    "Производитель",
    "Тип",
    "Размер дм.(м.)",
    "Диагональ экрана",
    "Соотношение сторон",
    "Формат",
    "Цвет",
    "Управление",
    "Полотно",
    "Корпус",
    "Вес в упак.",
    "Вес без упак.",
    "Габариты",
    "Страна происхождения",
    "Гарантия"
  ],
  "consumable": [
    "Производитель",
    "Модель",
    "Тип",
    "Тип печати",
    "Для бренда",
    "Совместимость",
    "Коды",
    "Цвет",
    "Ресурс",
    "Объем",
    "Объем, мл",
    "Количество в упаковке, шт.",
    "Страна происхождения",
    "Гарантия"
  ],
  "printer_mfp": [
    "Производитель",
    "Модель",
    "Тип",
    "Тип печати",
    "Цветность",
    "Тип чернил",
    "Тип расходных материалов",
    "Количество цветов",
    "Формат",
    "Максимальная скорость печати А4 стр/мин",
    "Разрешение печати,dpi",
    "Разрешение сканера,dpi",
    "Интерфейс",
    "Автоподатчик",
    "Автоматическая Двусторонняя печать",
    "Двусторонняя печать",
    "ЖК дисплей",
    "Область применения",
    "Назначение",
    "Минимальная плотность бумаги, г/м2",
    "Максимальная плотность бумаги, г/м2",
    "Коды",
    "Цвет корпуса",
    "Страна происхождения",
    "Гарантия",
    "Гарантия, мес"
  ],
  "shredder": [
    "Производитель",
    "Тип",
    "Тип резки",
    "Размер резки, мм",
    "Уровень секретности, DIN 66399",
    "Уровень секретности",
    "Загрузка, листов (A4, 70гр/м²)",
    "Производительность уничтожителя",
    "Рабочий цикл, мин",
    "Скорость резки, м/мин",
    "Уничтожение",
    "Уничтожение степлерных скоб",
    "Уничтожение скрепок",
    "Уничтожение кредитных карт",
    "Уничтожение CD или Blu-Ray DVD (1,2 слоя)",
    "Емкость корзины, л",
    "Емкость корзины, литров",
    "Вес, кг",
    "Габаритные размеры, мм (ВxШxГ)",
    "Страна происхождения",
    "Гарантия",
    "Гарантия, лет"
  ],
  "scanner": [
    "Производитель",
    "Модель",
    "Тип",
    "Разрешение",
    "Разрешение сканера, dpi",
    "Скорость сканирования",
    "Интерфейс",
    "Интерфейс USB",
    "Подключение по Wi-Fi",
    "Страна происхождения",
    "Гарантия"
  ],
  "laminator": [
    "Производитель",
    "Тип",
    "Макс. формат",
    "Толщина пленки, мкм",
    "Макс. скорость, см/мин",
    "Время нагрева, мин",
    "Страна происхождения",
    "Гарантия"
  ],
  "other": [
    "Производитель",
    "Модель",
    "Тип",
    "Коды",
    "Совместимость",
    "Цвет",
    "Страна происхождения",
    "Гарантия"
  ]
}

def _ac_schema_norm_key(k: str) -> str:
    k = (k or "").strip()
    if not k:
        return ""
    # единичные точечные правки
    k = k.replace("г/м2", "г/м²")
    k = k.replace(",dpi", ", dpi")
    k = k.replace("Дб", "дБ")
    k = k.replace("лмн", "лм")
    k = k.replace("(норм./ эконом.)", "(норм./эконом.)")
    # дубль-ключи
    if k == "Автоматическая Двусторонняя печать":
        k = "Двусторонняя печать"
    if k == "Разрешение печати,dpi":
        k = "Разрешение печати, dpi"
    if k == "Разрешение сканера,dpi":
        k = "Разрешение сканера, dpi"
    return k

def _ac_schema_norm_value(k: str, v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    # мусорное "Оригинальное"
    if k == "Оригинальное разрешение" and v.casefold() == "оригинальное":
        return ""
    # контрастность/соотношение сторон: "16 000 : 1" -> "16 000:1"
    if k in {"Контрастность", "Соотношение сторон"}:
        v = re.sub(r"\s*:\s*", ":", v)
        v = re.sub(r"\s+", " ", v).strip()
    # диапазоны: "1 - 1.2" -> "1-1.2"
    if k in {"Проекционный коэффициент"}:
        v = re.sub(r"\s*-\s*", "-", v)
    # разрешение: "1280х800" -> "1280×800"
    if k == "Разрешение":
        v = v.replace("х", "×").replace("x", "×")
    # Модель: если пришло название компании (Europe Ltd / GmbH / Inc / LLC) — убираем
    if k == "Модель":
        cf = v.casefold()
        if any(tok in cf for tok in [" ltd", "ltd.", " gmbh", " inc", " llc", " co.", " company", " europe ltd"]):
            return ""

    return v

def _ac_schema_word_count(s: str) -> int:
    return len([w for w in re.split(r"\s+", (s or "").strip()) if w])

_CODE_TOKEN_RE_SCHEMA = re.compile(r"\b[A-Z]{1,4}\d{2,}[A-Z0-9-]*\b")

def _ac_schema_is_bad_key(k: str) -> bool:
    if not k:
        return True
    if k in AKCENT_SCHEMA_DROP_KEY_EXACT:
        return True
    if len(k) > AKCENT_SCHEMA_MAX_KEY_LEN:
        return True
    if _ac_schema_word_count(k) > AKCENT_SCHEMA_MAX_KEY_WORDS:
        return True
    # если ключ выглядит как "название товара" (длинный и содержит кодовый токен) — выбрасываем
    if len(k) >= 40 and _CODE_TOKEN_RE_SCHEMA.search(k):
        return True
    return False

def _ac_schema_kind(name: str, params: list[tuple[str, str]]) -> str:
    name_cf = (name or "").casefold()
    tval = ""
    for k,v in params:
        if k == "Тип" and v:
            tval = v.casefold()
            break
    blob = (tval + " " + name_cf).strip()
    if "проектор" in blob:
        return "projector"
    if "экран" in blob:
        return "screen"
    if "шред" in blob or "уничтож" in blob:
        return "shredder"
    if "ламинатор" in blob:
        return "laminator"
    if "сканер" in blob:
        return "scanner"
    if "принтер" in blob or "мфу" in blob or "плоттер" in blob:
        return "printer_mfp"
    if any(x in blob for x in ["картридж", "тонер", "чернил", "чернила", "фотобарабан", "драм", "бумага", "этикет"]):
        return "consumable"
    return "other"

def _ac_apply_param_schema(name: str, params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    kind = _ac_schema_kind(name, params)
    allow_raw = AKCENT_SCHEMA_ALLOW_BY_KIND.get(kind) or []
    # нормализуем allow-список теми же правилами
    allow_set = {_ac_schema_norm_key(x) for x in allow_raw if x}
    # минимальный обязательный набор (на случай, если внезапно выпал из allow)
    allow_set |= {"Производитель","Модель","Тип","Гарантия","Страна происхождения","Коды","Совместимость","Цвет"}

    out: list[tuple[str, str]] = []
    seen: set[tuple[str,str]] = set()
    for k,v in params:
        k2 = _ac_schema_norm_key(k)
        if _ac_schema_is_bad_key(k2):
            continue
        v2 = _ac_schema_norm_value(k2, v)
        if not v2:
            continue
        if k2 not in allow_set:
            continue
        tup = (k2, v2)
        if tup in seen:
            continue
        seen.add(tup)
        out.append(tup)

    # порядок: сначала приоритетные, потом остальные по алфавиту ключа
    pri = [_ac_schema_norm_key(x) for x in (AKCENT_SCHEMA_PRIORITY_BY_KIND.get(kind) or []) if x]
    pri_idx = {k:i for i,k in enumerate(pri)}
    out.sort(key=lambda kv: (pri_idx.get(kv[0], 10**9), kv[0], kv[1]))
    return out
# ---------------------------

COUNTRY_VENDOR_BLACKLIST_CF = {
    "китай", "china",
    "россия", "russia",
    "казахстан", "kazakhstan",
    "турция", "turkey",
    "сша", "usa", "united states",
    "германия", "germany",
    "япония", "japan",
    "корея", "korea",
    "великобритания", "uk", "united kingdom",
    "франция", "france",
    "италия", "italy",
    "испания", "spain",
    "польша", "poland",
    "тайвань", "taiwan",
    "таиланд", "thailand",
    "вьетнам", "vietnam",
    "индия", "india",
}


def _clean_vendor(v: str) -> str:
    # vendor = бренд; если туда прилетает страна/общие слова — убираем, чтобы не портить бренд.
    s = (v or "").strip()
    if not s:
        return ""
    cf = s.casefold()

    # AkCent: иногда бренд приходит как 'Epson Proj' / 'ViewSonic proj' / '... projector'
    # Убираем хвост "proj"/"projector" и точки.
    s2 = re.sub(r"\s+(proj\.?|projector)\s*$", "", s, flags=re.IGNORECASE).strip()
    cf2 = s2.casefold()

    # Спец-кейс Epson (часто именно так и приходит)
    if cf in {"epson proj", "epson proj.", "epson projector"} or cf2 == "epson":
        return "Epson"

    # чистим "made in ..." и явные страны
    if "made in" in cf2 or cf2 in COUNTRY_VENDOR_BLACKLIST_CF:
        return ""

    return s2



# Приоритет характеристик (как в AlStyle: сначала важное, потом остальное по алфавиту)
AKCENT_PARAM_PRIORITY = [
    "Бренд",
    "Производитель",
    "Модель",
    "Артикул",
    "Тип",
    "Назначение",
    "Совместимость",
    "Коды",
    "Цвет",
    "Размер",
    "Материал",
    "Гарантия",
    "Интерфейс",
    "Подключение",
    "Разрешение",
    "Мощность",
    "Напряжение",
]

# Нормализуем URL (если вдруг пришёл без схемы)
def _normalize_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return u
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return "https://" + u.lstrip("/")

# Проверяем, что название товара начинается с одного из заданных префиксов
def _passes_name_prefixes(name: str) -> bool:
    s = (name or "").lstrip()
    if not s:
        return False
    s_cf = s.casefold()
    for pref_cf in AKCENT_NAME_PREFIXES_CF:
        if pref_cf and s_cf.startswith(pref_cf):
            return True
    return False


# Генерирует стабильный CS-oid для AkCent (offer id == vendorCode)
# Основной ключ: AC + offer@article (в XML он есть; в id оставляем только ASCII)
# Важно: если в article есть символы вроде "*", кодируем их как _2A, чтобы не ловить коллизии.
def _make_oid(offer: ET.Element, name: str) -> str | None:
    art = (offer.get("article") or "").strip()
    if art:
        out: list[str] = []
        for ch in art:
            if re.fullmatch(r"[A-Za-z0-9_.-]", ch):
                out.append(ch)
            else:
                out.append(f"_{ord(ch):02X}")
        part = "".join(out)
        if part:
            return "AC" + part    # fallback (на случай если поставщик поломает article)
    # ВАЖНО: никаких хэшей от имени — только стабильный id из исходных атрибутов.
    sid = (offer.get("id") or "").strip()
    if sid:
        out: list[str] = []
        for ch in sid:
            if re.fullmatch(r"[A-Za-z0-9_.-]", ch):
                out.append(ch)
            else:
                out.append(f"_{ord(ch):02X}")
        part = "".join(out)
        if part:
            return "AC" + part

    return None
# Берём текст узла (без None)
def _get_text(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()

# Собираем картинки
def _collect_pictures(offer: ET.Element) -> list[str]:
    pics: list[str] = []
    for p in offer.findall("picture"):
        t = _normalize_url(_get_text(p))
        if t:
            pics.append(t)
    # уникализация (сохраняем порядок)
    out: list[str] = []
    seen: set[str] = set()
    for u in pics:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

# Собираем параметры (param/Param)
def _collect_params(offer: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for p in offer.findall("param"):
        k = (p.get("name") or "").strip()
        v = _get_text(p)
        if k and v:
                # Мусор от поставщика: Гарантия=0 (убираем)
                if k.casefold() == 'гарантия' and v.strip().casefold() in ('0', '0 мес', '0 месяцев', '0мес'):
                    continue
                out.append((k, v))
    for p in offer.findall("Param"):
        k = (p.get("name") or p.get("Name") or "").strip()
        v = _get_text(p)
        if k and v:
                # Мусор от поставщика: Гарантия=0 (убираем)
                if k.casefold() == 'гарантия' and v.strip().casefold() in ('0', '0 мес', '0 месяцев', '0мес'):
                    continue
                out.append((k, v))
    return out

# Достаём vendor (если пусто — CS Core сам определит бренд по имени/парам/описанию)
def _extract_vendor(offer: ET.Element, params: list[tuple[str, str]], name: str = "", oid: str = "") -> str:
    v = _clean_vendor(_get_text(offer.find("vendor")))
    if v:
        return v
    for k, val in params:
        if k.casefold() in ("производитель", "бренд", "brand", "manufacturer"):
            v2 = _clean_vendor(val)
            if v2:
                return v2
    # фолбэк по oid/артикулу (если поставщик не дал vendor/производителя)
    oid_cf = (oid or "").casefold()
    if oid_cf:
        # Epson: большинство расходников AkCent кодируются как C13T... прямо в oid (например ACC13T00S64A)
        if "c13t" in oid_cf:
            return "Epson"
        # SMART: интерактивные панели/дисплеи часто идут SBID-... без явного бренда в названии
        if "sbid" in oid_cf:
            return "SMART"


    # фолбэк по имени (если поставщик не дал vendor/производителя в XML/params)
    n = (name or "").strip()
    if n:
        # порядок важен (самые специфичные выше)
        brand_map: list[tuple[re.Pattern, str]] = [
            (re.compile(r"\bMr\.?\s*Pixel\b", re.IGNORECASE), "Mr.Pixel"),
            (re.compile(r"\bView\s*Sonic\b", re.IGNORECASE), "ViewSonic"),
            (re.compile(r"\bSMART\b", re.IGNORECASE), "SMART"),
            (re.compile(r"\bSBID\b", re.IGNORECASE), "SMART"),
            (re.compile(r"\bIDPRT\b", re.IGNORECASE), "IDPRT"),
            (re.compile(r"\bFellowes\b", re.IGNORECASE), "Fellowes"),
            (re.compile(r"\bEpson\b", re.IGNORECASE), "Epson"),
            (re.compile(r"\bCanon\b", re.IGNORECASE), "Canon"),
            (re.compile(r"\bBrother\b", re.IGNORECASE), "Brother"),
            (re.compile(r"\bKyocera\b", re.IGNORECASE), "Kyocera"),
            (re.compile(r"\bRicoh\b", re.IGNORECASE), "Ricoh"),
            (re.compile(r"\bXerox\b", re.IGNORECASE), "Xerox"),
            (re.compile(r"\bLexmark\b", re.IGNORECASE), "Lexmark"),
            (re.compile(r"\bPantum\b", re.IGNORECASE), "Pantum"),
            (re.compile(r"\bSamsung\b", re.IGNORECASE), "Samsung"),
            (re.compile(r"\bToshiba\b", re.IGNORECASE), "Toshiba"),
            (re.compile(r"\bSharp\b", re.IGNORECASE), "Sharp"),
            (re.compile(r"\bOki\b", re.IGNORECASE), "OKI"),
            (re.compile(r"\bHP\b", re.IGNORECASE), "HP"),
        ]
        for rx, brand in brand_map:
            if rx.search(n):
                return brand

    # name-based fallback (если vendor/производитель не пришли)
    name_cf = (name or "").casefold()
    brand_map = [
        (r"\bhp\b", "HP"),
        (r"\bepson\b", "Epson"),
        (r"\bfellowes\b", "Fellowes"),
        (r"\bviewsonic\b", "ViewSonic"),
        (r"\bzebra\b", "Zebra"),
        (r"\bsmart\b", "SMART"),
        (r"\bmr\.pixel\b", "Mr.Pixel"),
        (r"\bidprt\b", "IDPRT"),
    ]
    for rx, outv in brand_map:
        if re.search(rx, name_cf, flags=re.IGNORECASE):
            return outv

    return ""


_ASPECT_FIX_NAME_RE = re.compile(r"^\s*Соотношение\s+сторон\s+(\d{1,2})\s*$", re.IGNORECASE)
_ASPECT_FIX_VAL_RE = re.compile(r"^\s*(\d{1,2})\s*$")

def _ac_fix_aspect_ratio_params(params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Фиксит битый кейс: <param name="Соотношение сторон 16">9</param> -> Соотношение сторон=16:9."""
    out: list[tuple[str, str]] = []
    for k, v in (params or []):
        k0 = (k or "").strip()
        v0 = (v or "").strip()
        m = _ASPECT_FIX_NAME_RE.match(k0)
        if m:
            m2 = _ASPECT_FIX_VAL_RE.match(v0)
            if m2:
                out.append(("Соотношение сторон", f"{m.group(1)}:{m2.group(1)}"))
                continue
        out.append((k, v))
    return out


# Достаём описание

# --- AkCent: максимум поставщик-специфичных правок в адаптере (CS-ready raw) ---

_AC_TEXT_REPL = [
    # орфография/типографика
    (r"конфернец", "конференц"),
    (r"характерстик", "характеристик"),
    (r"пурпурнымичернилами", "пурпурными чернилами"),
    (r"полотна(\d{3,4}\*)", r"полотна \1"),
]
_AC_TEXT_REPL_RE = [(re.compile(p, flags=re.IGNORECASE), rep) for p, rep in _AC_TEXT_REPL]

def _ru_minutes(n: int) -> str:
    # 1 минуту, 2-4 минуты, 5-20 минут, 21 минуту...
    n_abs = abs(int(n))
    n_mod100 = n_abs % 100
    n_mod10 = n_abs % 10
    if 11 <= n_mod100 <= 14:
        return "минут"
    if n_mod10 == 1:
        return "минуту"
    if 2 <= n_mod10 <= 4:
        return "минуты"
    return "минут"

def _ac_fix_text(desc: str) -> str:
    t = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    for rx, rep in _AC_TEXT_REPL_RE:
        t = rx.sub(rep, t)

    # типовые орфо/опечатки (AkCent)
    t = re.sub(r"(?i)\bтраспортировк", "транспортировк", t)
    t = re.sub(r"(?i)\bв\s+хранение\b", "в хранении", t)
    t = re.sub(r"(?i)\bв\s+комплект\s+работы\s+входит\b", "В комплект входит", t)

    # грамматика минут (склонение)
    def _min_repl(mm: re.Match) -> str:
        n = int(mm.group(1))
        return f"через {n} {_ru_minutes(n)}"
    t = re.sub(r"(?i)\bчерез\s+(\d+)\s+минут(?:ы|у)?\b", _min_repl, t)

    # ламинатор: изъять документ
    t = re.sub(r"(?i)\bдокумент\s+в\s+ламинатор\b", "документ из ламинатора", t)

    # 3LСD (кириллическая С) -> 3LCD
    t = t.replace("3LСD", "3LCD").replace("3lсd", "3lcd")

    # вырезаем огромные табличные простыни из описания (они пойдут в параметры)
    t = re.sub(
        r"(?is)\n\s*Технические\s+характеристики\s+Параметр/\s*\n\s*Значение\s*\n.*$",
        "",
        t,
    )

    # лечим оборванный хвост (встречается в AkCent): 'Уничтожение CD ... (1,'
    t = re.sub(
        r"(?is)\bУничтожение\s+CD\s+или\s+Blu-?Ray\s+DVD\s*\(1,\s*(?=$|\n)",
        "",
        t,
    )

    return t.strip()

def _ac_norm_name(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    # NBSP/узкие пробелы -> обычный пробел (иначе regex не ловит)
    s = s.replace("\u00A0", " ").replace("\u202F", " ")
    # пробел после ®
    s = re.sub(r"®\s*(?=[A-Za-z0-9])", "® ", s)
    # шредеры: 5лст/11 лтр -> 5 лист., 11 л
    s = re.sub(r"(?i)\b(\d+)\s*лст\.?\b", r"\1 лист.", s)
    s = re.sub(r"(?i)\b(\d+)\s*лтр\.?\b", r"\1 л", s)
    s = s.replace("лист..", "лист.")
    # размеры/десятичные: 2, 03 -> 2,03; X/× -> x
    s = re.sub(r"(\d),[ \t\u00A0\u202F]+(\d)", r"\1,\2", s)
    s = re.sub(r"[ \t\u00A0\u202F]+X[ \t\u00A0\u202F]+", " x ", s)
    s = s.replace("×", " x ")
    # запятые/пробелы
    s = re.sub(r",(\S)", r", \1", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()

def _ac_norm_country(v: str) -> str:
    t = (v or "").strip()
    if not t:
        return ""
    # приводим к запятым
    t = t.replace("/", ",").replace(";", ",")
    t = re.sub(r"\.\s*", ", ", t)
    t = re.sub(r"\s{2,}", " ", t)
    parts = [p.strip() for p in t.split(",") if p.strip()]
    normed = []
    for p in parts:
        p2 = p
        p2 = re.sub(r"(?i)\bжапония\b", "Япония", p2)
        p2 = re.sub(r"(?i)\bфилипин(ы|)\b", "Филиппины", p2)
        p2 = re.sub(r"(?i)\bфилиппин\b", "Филиппины", p2)
        p2 = re.sub(r"(?i)\bфилипины\b", "Филиппины", p2)
        p2 = p2[:1].upper() + p2[1:] if p2 else p2
        if p2 and p2 not in normed:
            normed.append(p2)
    return ", ".join(normed)

def _ac_extract_tab_specs_from_desc(desc: str) -> tuple[list[tuple[str, str]], str]:
    """Вытаскиваем табличные строки AkCent вида 'Ключ\\tЗначение' в params и вычищаем их из описания."""
    t = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    if "\t" not in t:
        return [], (t.strip())
    lines = t.split("\n")
    out: list[tuple[str, str]] = []
    keep: list[str] = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        if "\t" in ln:
            left, right = ln.split("\t", 1)
            k = left.strip()
            v = right.strip()
            # если значение пустое, собираем следующие строки пока не встретим новую таб-пару или пустую строку
            if k and not v:
                vals = []
                j = i + 1
                while j < len(lines):
                    ln2 = lines[j]
                    if "\t" in ln2:
                        break
                    if ln2.strip() == "":
                        break
                    vals.append(ln2.strip())
                    j += 1
                v = ", ".join(dict.fromkeys(vals)) if vals else ""
                i = j - 1
            if k and v:
                out.append((k, v))
            # не добавляем эту строку в описание
        else:
            keep.append(ln)
        i += 1
    cleaned = "\n".join(keep).strip()
    return out, cleaned

_CODE_TOKEN_RE = re.compile(r"\bC13T\d{5,8}[A-Z]?\b"r"|\bC12C\d{6}\b"r"|\bC11[A-Z]{2}\d{5}[A-Z0-9]{0,2}\b"r"|\bV1[23]H[0-9A-Z]{6,12}\b"r"|\bC\d{2}C\d{5,6}\b"r"|\b(?:CE|CF|CC|CB|Q)\d{3,6}[A-Z]?\b"r"|\b106R\d{5}\b"r"|\b(?:TN|DR|TK)\s*-?\s*\d{3,5}[A-Z]?\b"r"|\bMLT\s*-?\s*[A-Z]?\d{3,4}[A-Z]?\b"r"|\bCRG\s*-?\s*\d{3,4}[A-Z]?\b"r"|\bW\d{4}[A-Z]\b"r"|\bT\d{2}[A-Z]?\b"r"|\b[A-Z]\d{2}[A-Z]\d{3,6}\b", re.IGNORECASE)
def _ac_extract_codes_from_fields(name: str, params: list[tuple[str, str]], desc: str) -> list[str]:
    text = " ".join([name or "", desc or ""] + [f"{k} {v}" for k, v in (params or [])])
    codes = []
    for m in _CODE_TOKEN_RE.finditer(text):
        c = m.group(0).upper()
        if c not in codes:
            codes.append(c)
    return codes

def _ac_extract_volume_ml(name: str, desc: str, params: list[tuple[str, str]]) -> str:
    text = " ".join([name or "", desc or ""] + [v for _, v in (params or [])])
    m = re.search(r"(?i)\b(\d{2,4})\s*(мл|ml)\b", text)
    if m:
        return f"{m.group(1)} мл"
    return ""


_LAT2CYR = str.maketrans({
    "A":"А","a":"а","B":"В","E":"Е","e":"е","K":"К","k":"к","M":"М","m":"м",
    "H":"Н","h":"н","O":"О","o":"о","P":"Р","p":"р","C":"С","c":"с",
    "T":"Т","t":"т","X":"Х","x":"х","Y":"У","y":"у"
})

def _ac_cyr_like(s: str) -> str:
    # приводим латинские "похожие" буквы к кириллице для устойчивых замен (только внутри AkCent)
    return (s or "").translate(_LAT2CYR)

def _ac_params_postfix(params: list[tuple[str, str]], name: str, desc: str) -> list[tuple[str, str]]:
    out = []
    # rename keys / values
    for k, v in params:
        kk = (k or "").strip()
        vv = (v or "").strip()
        if not kk or not vv:
            continue
        kcf = kk.casefold()
        # Keystone manual labels from specs
        if kcf in {"вручную vertical", "manual vertical"}:
            return "Коррекция трапецеидальных искажений"
        # ключи
        if kcf == "проекционный коэффицент (throw ratio)" or kcf == "проекционный коэффицент":
            kk = "Проекционный коэффициент"
        elif kcf == "тип резки":
            kk = "Тип резки"
        # значения
        if kk.casefold() == "уничтожение":
            vv_norm = _ac_cyr_like(vv)
            vv_norm = re.sub(r"(?i)\bскобк[ыи]\b", "скобы", vv_norm)
            vv = vv_norm
        if kk.casefold() == "страна происхождения":
            vv = _ac_norm_country(vv)
        if kk.casefold().startswith("отдельная корзина") and vv.casefold() in {"н", "н.", "нету", "нет"}:
            vv = "нет"
        out.append((kk, vv))
    # Совместимость из табличных параметров вида "Epson L7160"="C11..." и т.п.
    compat = []
    cleaned2 = []
    for k, v in out:
        if re.match(r"(?i)^(epson|hp|canon|brother|xerox|panasonic|ricoh|kyocera)\b", k.strip()):
            # если значение похоже на код/артикул производителя, считаем это строкой совместимости
            if re.search(r"\bC\d{2,}\b", v) or re.search(r"\b[A-Z]{1,2}\d{3,}\b", v) or v.strip().endswith("-"):
                compat.append(k.strip())
                continue
        cleaned2.append((k, v))
    out = cleaned2
    if compat:
        compat_u = []
        for c in compat:
            if c not in compat_u:
                compat_u.append(c)
        out.append(("Совместимость", ", ".join(compat_u)))
    # Коды расходников (AkCent): оставляем только реальные коды расходников, а модели (T3000/T5200/...) убираем
    existing_vals: list[str] = []
    tmp: list[tuple[str, str]] = []
    for k, v in out:
        if k.casefold() in ("коды расходников", "коды"):
            if v:
                existing_vals.append(v)
        else:
            tmp.append((k, v))
    out = tmp

    def _codes_from_val(vv: str) -> list[str]:
        toks = re.split(r"[;,\s]+", (vv or ""))
        res: list[str] = []
        for t in toks:
            tt = t.strip().strip(".,:()[]{}<>")
            if not tt:
                continue
            uu = tt.upper()
            if uu.isdigit():
                continue
            # это модели устройств, не коды расходников (Epson SureColor T3000/T5200/T5700D и т.п.)
            if re.fullmatch(r"T\d{3,4}[A-Z]?", uu):
                continue
            # допускаем типовые коды (Epson C13T..., HP/Canon вида CE278A и т.п., а также T09/T11 и т.п.)
            if re.fullmatch(r"C13T\d{5,6}[A-Z]?", uu) or re.fullmatch(r"[A-Z]\d{2}[A-Z]\d{3,6}", uu) or re.fullmatch(r"W\d{4}[A-Z]", uu) or re.fullmatch(r"T\d{2}[A-Z]?", uu):
                if uu not in res:
                    res.append(uu)
        return res

    codes: list[str] = []
    for vv in existing_vals:
        for c in _codes_from_val(vv):
            if c not in codes:
                codes.append(c)

    # добираем коды из name/desc (только по безопасным паттернам)
    for c in _ac_extract_codes_from_fields(name, out, desc):
        cc = c.upper()
        if re.fullmatch(r"T\d{3,4}[A-Z]?", cc):
            continue
        if cc not in codes:
            codes.append(cc)

    if codes:
        out.append(("Коды", ", ".join(codes)))
    # Ресурс (только для расходников)
    name_cf = (name or "").casefold()
    if any(w in name_cf for w in ["чернила", "картридж", "тонер", "драм", "drum", "ink", "toner", "cartridge"]):
        vol = _ac_extract_volume_ml(name, desc, out)
        if vol:
            out.append(("Ресурс", vol))
    return out


def _extract_desc(offer: ET.Element) -> str:
    return _get_text(offer.find("description"))

# Достаём исходную цену:
# AkCent кладёт цены в <prices><price type="Цена дилерского портала KZT">41727</price> ...</prices>
def _extract_price_in(offer: ET.Element) -> int:
    prices = offer.find("prices")
    if prices is not None:
        best_any: int | None = None
        best_rrp: int | None = None
        for pe in prices.findall("price"):
            t = (pe.get("type") or "").casefold()
            cur = (pe.get("currencyId") or "").strip().upper()
            v = safe_int(_get_text(pe))
            if not v:
                continue
            if cur and cur != "KZT":
                continue

            # 1) приоритет — дилерская цена
            if "дилер" in t or "dealer" in t:
                return int(v)

            # 2) RRP как запасной приоритет
            if "rrp" in t:
                best_rrp = int(v)

            if best_any is None:
                best_any = int(v)

        if best_rrp is not None:
            return best_rrp
        if best_any is not None:
            return best_any

    # запасные варианты (на случай другого формата)
    p1 = safe_int(_get_text(offer.find("purchase_price")))
    if p1:
        return int(p1)
    p2 = safe_int(_get_text(offer.find("price")))
    return int(p2 or 0)

# Достаём доступность (если нет атрибута — считаем true)
def _extract_available(offer: ET.Element) -> bool:
    a = (offer.get("available") or "").strip().lower()
    if not a:
        return True
    return a in ("1", "true", "yes", "y", "да")

# Вытаскиваем offers из XML
def _extract_offers(root: ET.Element) -> list[ET.Element]:
    offers_node = root.find(".//offers")
    if offers_node is None:
        return []
    return list(offers_node.findall("offer"))

def _next_run_almaty(build_time: str, hour: int) -> str:
    """Ближайшая сборка по Алматы: следующий запуск сегодня/завтра в hour:00:00.
    build_time — строка now_almaty() вида 'YYYY-MM-DD HH:MM:SS'.
    """
    try:
        from datetime import datetime, timedelta

        dt = datetime.strptime((build_time or "").strip(), "%Y-%m-%d %H:%M:%S")
        cand = dt.replace(hour=int(hour), minute=0, second=0, microsecond=0)
        if cand <= dt:
            cand = cand + timedelta(days=1)
        return cand.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # fallback на core-хелпер (если формат времени вдруг поменяется)
        return next_run_at_hour(build_time, int(hour))

# main
def main() -> int:
    print(f"[akcent] version={BUILD_AKCENT_VERSION}")
    build_time = now_almaty()
    next_run = _next_run_almaty(build_time, 2)
    r = requests.get(_normalize_url(SUPPLIER_URL), timeout=90)
    r.raise_for_status()
    root = _xml_from_bytes_safe(r.content)
    offers_in = _extract_offers(root)
    before = len(offers_in)

    out_offers: list[OfferOut] = []

    price_missing = 0

    for offer in offers_in:
        name = _ac_norm_name(_get_text(offer.find("name")))
        if not name or not _passes_name_prefixes(name):
            continue

        # CS: выкидываем "картриджи для фильтра/бутылки" (Philips AWP) из ассортимента
        art_raw = (offer.get("article") or "").strip()
        if art_raw in AKCENT_DROP_ARTICLES:
            continue
        ncf = (name or "").casefold()
        if ("картридж" in ncf or "cartridge" in ncf) and ("фильтр" in ncf or "filter" in ncf or "бутылк" in ncf or "bottle" in ncf) and ("philips" in ncf or "awp" in ncf):
            continue

        oid = _make_oid(offer, name)
        if not oid:
            continue
        if not oid:
            continue

        available = _extract_available(offer)
        pics = _collect_pictures(offer)
        params_raw = _collect_params(offer)
        native_desc = _ac_fix_text(_extract_desc(offer))
        extra_params2, native_desc = _ac_extract_colon_specs_from_desc(native_desc)
        extra_params, native_desc = _ac_extract_tab_specs_from_desc(native_desc)
        if extra_params2:
            params_raw.extend(extra_params2)
        if extra_params:
            params_raw.extend(extra_params)
        params_raw = _ac_params_postfix(params_raw, name, native_desc)
        params = clean_params(params_raw, drop=AKCENT_PARAM_DROP)

        price_in = _extract_price_in(offer)
        if not price_in or int(price_in) < 1:
            price_missing += 1
        price = compute_price(price_in)
        vendor = _extract_vendor(offer, params, name, oid)

        params = _ac_enrich_codes_and_compat(oid, name, vendor, params, native_desc)
        params = _ac_fix_model_by_name(name, vendor, params)
        params = _ac_fix_aspect_ratio_params(params)
        params = clean_params(params, drop=AKCENT_PARAM_DROP)
        params = _ac_apply_param_schema(name, params)
        out_offers.append(
            OfferOut(
                oid=oid,
                available=available,
                name=name,
                price=price,
                pictures=pics,
                vendor=vendor,
                params=params,
                native_desc=native_desc,
            )
        )

    after = len(out_offers)
    in_true = sum(1 for o in out_offers if o.available)
    in_false = after - in_true

    public_vendor = get_public_vendor()

    # Стабильный порядок офферов (меньше лишних диффов между коммитами)
    out_offers.sort(key=lambda x: x.oid)

    write_cs_feed_raw(out_offers, supplier=SUPPLIER_NAME, supplier_url=SUPPLIER_URL, out_file="docs/raw/akcent.yml", build_time=build_time, next_run=next_run, before=before, encoding=OUTPUT_ENCODING, currency_id="KZT")

    changed = write_cs_feed(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL,
        out_file=OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        public_vendor=public_vendor,
        currency_id="KZT",
        param_priority=AKCENT_PARAM_PRIORITY,
    )

    print(f"[akcent] before={before} after={after} price_missing={price_missing} changed={changed}")

    return 0
def _ac_extract_colon_specs_from_desc(desc: str) -> tuple[list[tuple[str, str]], str]:
    """Извлекает характеристики из многострочного описания вида 'Ключ: Значение'.

    Важно (фикс под AkCent):
    - не превращаем строки-отношения вида '16:9', '5.000.000:1', '10 : 1' в мусорные параметры (key='16', value='9')
    - поддерживаем связку "Aspect Ratio" -> следующая строка (значение) как 'Соотношение сторон'
      и аналогично для Contrast/Throw Ratio/Offset.
    """
    if not desc:
        return [], desc

    lines = desc.splitlines()
    out_params: list[tuple[str, str]] = []
    out_lines: list[str] = []

    # Заголовки секций (обычно без ':') — не сохраняем как характеристики
    section_headers = {
        "общие параметры", "изображение", "интерфейсы", "корпус", "разъемы", "питание",
        "функции", "другое", "экран", "сеть", "память", "звук",
    }

    # Англ. заголовки из Epson-спеков -> куда кладём значение следующей строкой
    pending_map = {
        "aspect ratio": "Соотношение сторон",
        "contrast ratio": "Контрастность",
        "throw ratio": "Проекционный коэффициент",
        "offset": "Смещение",
        "noise level": "Уровень шума (норм./эконом.)",
        "loudspeaker": "Динамик",
    }
    pending_key: str = ""

    def is_good_key(k: str) -> bool:
        k = (k or "").strip()
        if not k or len(k) > 70:
            return False
        kcf = k.casefold()
        if kcf in section_headers:
            return False
        # не тащим URL как ключ
        if "http" in kcf:
            return False
        # ключ должен содержать буквы (иначе получаем мусор вроде key='16')
        if not re.search(r"[A-Za-zА-Яа-я]", k):
            return False

        # не принимаем ключи, начинающиеся с цифры (мусор вроде "5 Watt, Stereo")
        if re.match(r"^\d", k):
            return False

        # явный мусор из Epson-спеков
        if kcf in {"from", "to", "normal", "digital, factor", "yes", "no"}:
            return False

        # если ключ полностью на латинице — разрешаем только небольшую whitelist (иначе мусор вроде "Normal")
        if re.search(r"[A-Za-z]", k) and not re.search(r"[А-Яа-яЁё]", k):
            if not re.fullmatch(r"(?i)(3d|usb|wi-?fi|ethernet\s*\(rj-45\)|rs-?232|hdmi|vga\s*\(d-sub\)|vga|dvi-d|displayport|adaptive\s+sync|freesync)", k.strip()):
                return False

        return True

    def is_good_val(v: str) -> bool:
        v = (v or "").strip()
        if not v:
            return False
        if len(v) > 250:
            return False
        return True

    extracted = 0
    for ln in lines:
        s = (ln or "").strip()
        if not s:
            out_lines.append(ln)
            continue

        s_cf = s.casefold()

        # 1) Заголовок-ожидание (англ. спек)
        if s_cf in pending_map:
            pending_key = pending_map[s_cf]
            # заголовок в текст не пишем (чтобы не мусорить описание)
            continue

        # 2) Следующая строка после заголовка — значение
        if pending_key:
            # нормализация "10 : 1" -> "10:1"
            v = re.sub(r"\s*:\s*", ":", s).strip()

            # спец-нормализация для Epson-спеков
            if pending_key == "Уровень шума (норм./эконом.)":
                # "Normal: 22 dB (A) - Economy: 18 dB (A)" -> "22 dB (A) / 18 dB (A)"
                v = re.sub(r"(?i)^normal\s*:\s*", "", v).strip()
                v = re.sub(r"(?i)\s*-\s*economy\s*:\s*", " / ", v).strip()
            elif pending_key == "Динамик":
                # "5 Watt, Stereo:Stereo" -> "5 Вт, стерео"
                v = re.sub(r"(?i)\bwatt\b", "Вт", v)
                v = re.sub(r"(?i),\s*stereo", ", стерео", v)
                v = re.sub(r"(?i):\s*stereo\s*$", "", v).strip()

            # принимаем как значение, если есть цифры
            if re.search(r"\d", v):
                pair = (pending_key, v)
                if pair not in out_params:
                    out_params.append(pair)
                extracted += 1
                pending_key = ""
                continue
            pending_key = ""

        # 3) Отдельные строки-отношения вида "16:9" / "5.000.000:1" — НЕ парсим как "ключ:значение"
        if re.fullmatch(r"\d[\d\s.,-]*:\s*\d[\d\s.,-]*", s):
            out_lines.append(ln)
            continue

        # 4) Обычные строки "Ключ: Значение"
        if ":" in s and not s.startswith("http"):
            k, v = s.split(":", 1)
            k = k.strip()
            v = v.strip()

            # спец-кейс: "Контрастность 16 000:1" / "Контрастность 3 000 000:1 (динамическая)"
            if re.match(r"^1(\s|$|\()", v):
                kcf = k.casefold().replace("ё", "е")
                if ("контраст" in kcf or "contrast" in kcf) and re.search(r"\d", k):
                    digits = re.sub(r"[^0-9\s.,]", "", k).strip()
                    if digits:
                        is_dyn = ("dynamic" in kcf) or ("динамичес" in kcf) or ("динамичес" in v.casefold())
                        k = "Динамическая контрастность" if is_dyn else "Контрастность"
                        digits2 = re.sub(r"\s{2,}", " ", digits)
                        v = digits2 + ":1"

            if is_good_key(k) and is_good_val(v):
                pair = (k, v)
                if pair not in out_params:
                    out_params.append(pair)
                extracted += 1
                # строку выкидываем из текста
                if extracted >= 80:
                    out_lines.extend(lines[len(out_lines):])
                    break
                continue

        out_lines.append(ln)

    cleaned = "\n".join(out_lines).strip()
    return out_params, cleaned


def _ac_norm_code_token(t: str) -> str:
    s = (t or "").strip().upper()
    if not s:
        return ""
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s

def _ac_split_list(v: str) -> list[str]:
    if not v:
        return []
    t = (v or "").replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"\s+/\s+", ",", t)
    parts = re.split(r"[,;\n]+", t)
    out: list[str] = []
    for p in parts:
        pp = p.strip().strip(".,:()[]{}<>")
        if pp:
            out.append(pp)
    return out

def _ac_is_code_only_list(v: str) -> bool:
    items = _ac_split_list(v)
    if not items:
        return False
    for it in items:
        tt = _ac_norm_code_token(it)
        if not tt or not _CODE_TOKEN_RE.fullmatch(tt):
            return False
    return True

def _ac_key_looks_like_model(k: str) -> bool:
    s = (k or "").strip()
    if not s:
        return False
    if len(s) > 80:
        return False
    if not re.search(r"\d", s):
        return False
    return bool(re.search(r"(?i)\b(epson|hp|canon|brother|xerox|kyocera|ricoh|konica|minolta|samsung|pantum|oki|lexmark|sharp)\b", s))

def _ac_norm_interface_value(v: str) -> str:
    t = (v or "").strip()
    if not t:
        return ""
    t = re.sub(r"\s*\*\s*", " / ", t)
    t = re.sub(r"\s*/\s*", " / ", t)
    t = re.sub(r"\s{2,}", " ", t).strip(" /")
    return t

def _ac_enrich_codes_and_compat(oid: str, name: str, vendor: str, params: list[tuple[str, str]], desc: str) -> list[tuple[str, str]]:
    """Adapter-first финальная нормализация параметров AkCent:
    - Интерфейс/Подключение: '*' -> '/'
    - Если 'Совместимость' содержит только коды -> перенос в 'Коды'
    - 'Коды расходников' -> 'Коды'
    - Если param-name выглядит как модель, а value как код -> модель в 'Совместимость', код в 'Коды'
    """
    out: list[tuple[str, str]] = []
    codes_accum: list[str] = []
    compat_accum: list[str] = []

    for k, v in (params or []):
        k0 = (k or "").strip()
        v0 = (v or "").strip()
        if not k0 or not v0:
            continue

        kcf = k0.casefold()


        # Диапазоны '...'
        v0 = _ac_norm_ranges(v0)

        # Чистим производителя (Epson Proj -> Epson)
        if kcf == "производитель":
            v0 = _clean_vendor(v0)
        if kcf in {"интерфейс", "интерфейсы", "подключение"}:
            v0 = _ac_norm_interface_value(v0)

        # "Epson L7160" = "C11CG15404"
        if _ac_key_looks_like_model(k0) and _CODE_TOKEN_RE.fullmatch(_ac_norm_code_token(v0)):
            if k0 not in compat_accum:
                compat_accum.append(k0)
            c = _ac_norm_code_token(v0)
            if c and c not in codes_accum:
                codes_accum.append(c)
            continue

        if kcf == "совместимость" and _ac_is_code_only_list(v0):
            for c in _ac_split_list(v0):
                cc = _ac_norm_code_token(c)
                if cc and cc not in codes_accum:
                    codes_accum.append(cc)
            continue

        if kcf in {"коды", "коды расходников"}:
            for c in _ac_split_list(v0):
                cc = _ac_norm_code_token(c)
                if cc and cc not in codes_accum:
                    codes_accum.append(cc)
            continue

        out.append((k0, v0))

    # Добираем коды из имени/описания/oid (мягко)
    text_for_codes = " ".join([oid or "", name or "", desc or ""])
    for c in _ac_extract_codes_from_fields(text_for_codes, out, vendor or ""):
        cc = _ac_norm_code_token(c)
        if cc and cc not in codes_accum:
            codes_accum.append(cc)

    if compat_accum:
        out.append(("Совместимость", ", ".join(compat_accum[:40])))
    if codes_accum:
        out.append(("Коды", ", ".join(codes_accum[:80])))

    return out


_SC_T_CODE_RE = re.compile(r"\bSC-T\d{4,5}[A-Z]?\b", re.IGNORECASE)


def _ac_fix_model_by_name(name: str, vendor: str, params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Фикс редкого, но критичного кейса AkCent:
    в названии одна модель (например SC-T5700D), а в param 'Модель' приходит другая (SC-T5200).

    Правило: если vendor=Epson и в name есть код SC-T..., то 'Модель' должна содержать этот код.
    """
    nm = (name or "").strip()
    if not nm:
        return params
    if (vendor or "").casefold() != "epson":
        return params

    mm = _SC_T_CODE_RE.search(nm)
    if not mm:
        return params

    code = (mm.group(0) or "").upper()
    if not code:
        return params

    prefix = "Epson SureColor" if "surecolor" in nm.casefold() else "Epson"
    desired = f"{prefix} {code}".strip()

    out: list[tuple[str, str]] = []
    found_model = False
    for k, v0 in (params or []):
        if (k or "").strip().casefold() == "модель":
            found_model = True
            cur = (v0 or "").strip()
            if code not in cur.upper():
                out.append(("Модель", desired))
            else:
                out.append(("Модель", cur))
        else:
            out.append((k, v0))

    if not found_model:
        out.append(("Модель", desired))

    return out

def _ac_norm_ranges(v: str) -> str:
    """Нормализует диапазоны вида '5...40' -> '5–40'."""
    s = (v or "").strip()
    if not s:
        return ""
    # 5...40 -> 5–40 ; 1.19...1.61 -> 1.19–1.61
    s = re.sub(r"(\d(?:[\d.,]*\d)?)\s*\.\.\.\s*(\d(?:[\d.,]*\d)?)", r"\1–\2", s)
    # иногда встречается '... ' без пробелов
    s = s.replace("…", "…")  # keep unicode ellipsis as-is
    return s



def _ac_norm_ranges(v: str) -> str:
    """Нормализует диапазоны вида '5...40' -> '5–40', '1.19...1.61' -> '1.19–1.61'."""
    s = (v or "").strip()
    if not s:
        return ""
    # 5...40 -> 5–40 ; 1.19...1.61 -> 1.19–1.61
    s = re.sub(r"(\d(?:[\d.,]*\d)?)\s*\.\.\.\s*(\d(?:[\d.,]*\d)?)", r"\1–\2", s)
    # Warranty: if only digits, assume months
    if (k or '').casefold() in {'гарантия', 'гарантия, мес', 'гарантия (мес)'}:
        vv = (s or '').strip()
        if re.fullmatch(r'\d{1,3}', vv):
            return f"{int(vv)} мес"

    # _keystone: normalize combined vertical/horizontal manual correction
    if (k or '').casefold() == 'коррекция трапецеидальных искажений':
        vv = (s or '').strip()
        m_v = re.search(r'±\s*\d+\s*°', vv)
        m_h = re.search(r'горизонт\w*\s*±\s*\d+\s*°', vv, flags=re.IGNORECASE)
        if m_v or m_h:
            vpart = m_v.group(0).replace(' ', '') if m_v else ''
            hdeg = re.search(r'±\s*\d+\s*°', m_h.group(0)).group(0).replace(' ', '') if m_h else ''
            parts = []
            if vpart: parts.append(f"вертикальная {vpart}")
            if hdeg: parts.append(f"горизонтальная {hdeg}")
            if parts:
                return ', '.join(parts)

    return s

if __name__ == "__main__":
    raise SystemExit(main())
