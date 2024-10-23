## Домашние задания по курсу "Разработка систем анализа больших данных"

Каждое задание выполнено в отдельном каталоге, именуемом lab1, lab2 и так далее.

## Домашнее задание №1
### Разработка пайплайна с помощью фреймворка Luigi

### 1. Пайплайн включает следующие задачи:
 - GetDataset: создание какталога для необработанных данных (по умолчанию ```/data_row```) и скачивание в него набора данных;
 - UnzipDataset: распаковка архива с данными в каталог с необработанными данными (по умолчанию ```/data_row```);
 - UnzipFiles: создание каталогов для каждого gz файла и разархивирования gz файлов в каталоге с необработанными данными (по умолчанию ```/data_row```);
 - SplitFiles: создание каталога для обработанных данных (по умолчанию ```/data```), разделение файлов с tsv-таблицами на отдельные таблицы, удаление колонок из таблицы  и сохранение каждой таблицы ```Probes``` в отдельный файл;
 - ZipDataset: создание архима с обработанными файлами в каталоге для обработанных данных (по умолчанию ```/data```).

### 2. Параметры пайплайна:
 - dataset-name: имя набора данных;
 - data-path: имя каталога для обработанных данных (по умолчанию ```/data```);
 - data-row-path: имя каталога для необработанных данных (по умолчанию ```/data_row```).

### 3. Пример запуска пайплайна:
```
python -m pipeline ZipDataset --dataset-name=GSE68849 --data-path=data --data-row-path=data_row --local-schedule
```