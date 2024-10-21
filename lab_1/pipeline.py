import gzip
import io
import luigi
import os
import tarfile
import wget
import pandas as pd
import zipfile
from pathlib import Path


class GetDataset(luigi.Task):
  """Класс, реализующий загрузки набора данных"""

  dataset_name = luigi.Parameter()
  data_row_path = luigi.Parameter(default='data_row')

  dataset_row_ext = '.tar'

  def get_dataset_row_file(self):
    return f'{self.data_row_path}/{self.dataset_name}{self.dataset_row_ext}'

  def run(self):
    dataset_row_file = self.get_dataset_row_file();
    os.makedirs(f'{self.data_row_path}', exist_ok=True)

    if not os.path.exists(dataset_row_file):
      url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file'
      wget.download(url, dataset_row_file)

  def output(self):
    dataset_row_file = self.get_dataset_row_file();
    return luigi.LocalTarget(dataset_row_file)


class UnzipDataset(luigi.Task):
  """Класс задачи разархивирования набора данных"""

  dataset_name = luigi.Parameter()
  data_row_path = luigi.Parameter(default='data_row')

  files_gz = [];

  def get_dataset_row_dir(self):
    return f'{self.data_row_path}/{self.dataset_name}'

  def requires(self):
    return GetDataset(self.dataset_name, self.data_row_path)

  def run(self):
    dataset_row_dir = self.get_dataset_row_dir()
    dataset_row_file = str(self.input())

    with tarfile.open(dataset_row_file) as file_input:
      self.files_gz = file_input.getnames()
      file_input.extractall(dataset_row_dir)
      file_input.close()

  def output(self):
    dataset_row_dir = self.get_dataset_row_dir()
    return [ luigi.LocalTarget(f'{dataset_row_dir}/{file_gz}') for file_gz in self.files_gz ]


class UnzipFiles(luigi.Task):
  """Класс, реализующий создание каталогов и разархивирования gz файлов с данными"""

  dataset_name = luigi.Parameter()
  data_row_path = luigi.Parameter(default='data_row')

  files_tsv = [];

  def get_dataset_row_dir(self):
    return f'{self.data_row_path}/{self.dataset_name}'

  def requires(self):
    return UnzipDataset(self.dataset_name, self.data_row_path)

  def run(self):
    for file_path in self.input():
      dataset_row_file = str(file_path)
      dataset_row_dir = self.get_dataset_row_dir()

      if Path(dataset_row_file).is_file() and Path(dataset_row_file).suffix == '.gz':
        file_name = dataset_row_file.split('/')[-1].split('.')[0]
        os.makedirs(f'{dataset_row_dir}/{file_name}', exist_ok=True)

        with gzip.open(dataset_row_file, 'rt') as file_input:
          file_data = file_input.read()

          with open(f'{dataset_row_dir}/{file_name}/{file_name}.tsv', 'w') as file_output:
            self.files_tsv.append(f'{dataset_row_dir}/{file_name}/{file_name}.tsv')
            file_output.write(file_data)
            file_output.close()

          file_input.close()

  def output(self):
    return [ luigi.LocalTarget(file_tsv) for file_tsv in self.files_tsv ]


class SplitFiles(luigi.Task):
  """Класс, реализующий разделение файла с данными на отдельные файлы"""

  dataset_name = luigi.Parameter()
  data_path = luigi.Parameter(default='data')
  data_row_path = luigi.Parameter(default='data_row')

  files_tsv = []

  def get_dataset_dir(self):
    return f'{self.data_path}/{self.dataset_name}'

  def requires(self):
    return UnzipFiles(self.dataset_name, self.data_row_path)

  def split_tables(self, file_path):
    dfs = {}

    with open(file_path) as file_input:
      write_key = None
      fio = io.StringIO()

      for line in file_input.readlines():
        if line.startswith('['):
          if write_key:
            fio.seek(0)
            header = None if write_key == 'Heading' else 'infer'
            dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)

          fio = io.StringIO()
          write_key = line.strip('[]\n')
          continue
        if write_key:
          fio.write(line)

      fio.seek(0)
      dfs[write_key] = pd.read_csv(fio, sep='\t')   

    return dfs

  def run(self):
    dataset_dir = self.get_dataset_dir()
    os.makedirs(dataset_dir, exist_ok=True)

    columns=[
      'Definition', 'Ontology_Component', 'Ontology_Process',
      'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence'
    ]

    for file_path in self.input():
      file_name = str(file_path).split('/')[-1].split('.')[0]
      os.makedirs(f'{dataset_dir}/{file_name}', exist_ok=True)

      dfs = self.split_tables(str(file_path))

      for header, dataframe in dfs.items():
        file_tsv = f'{dataset_dir}/{file_name}/{file_name}_{header}.tsv'
        self.files_tsv.append(file_tsv)

        dataframe.to_csv(file_tsv, sep='\t', index=False)

        if header == 'Probes':
          df_wc = dataframe.drop(columns, axis=1)

          file_tsv_wc = f'{dataset_dir}/{file_name}/{file_name}_{header}_WC.tsv'
          self.files_tsv.append(file_tsv_wc)

          df_wc.to_csv(file_tsv_wc, sep='\t', index=False)

  def output(self):
    return [ luigi.LocalTarget(file) for file in self.files_tsv ]


class ZipDataset(luigi.Task):
  """Класс задачи архивирования файлов с обработанными данными"""

  dataset_name = luigi.Parameter()
  data_path = luigi.Parameter(default='data')
  data_row_path = luigi.Parameter(default='data_row')

  def get_dataset_dir(self):
    return f'{self.data_path}/{self.dataset_name}'

  def requires(self):
    return SplitFiles(self.dataset_name, self.data_path, self.data_row_path)

  def run(self):
    data_dir = self.get_dataset_dir()

    with zipfile.ZipFile(f'{data_dir}.zip', "w") as file_output:
      for file_path in self.input():
        file_arc_name = '/'.join(str(file_path).split('/')[2:])
        file_output.write(str(file_path), arcname=file_arc_name, compress_type=zipfile.ZIP_DEFLATED)
      file_output.close()


if __name__ == '__main__':
  luigi.run()