import gzip
import luigi
import os
import tarfile
import wget


class GetDataset(luigi.Task):
  dataset = luigi.Parameter()
  data_row_path = luigi.Parameter(default='data_row')

  def run(self):
    url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset}&format=file'
    wget.download(url, f'{self.data_row_path}/{self.dataset}.tar')

  def output(self):
    return luigi.LocalTarget(f'{self.dataset}.tar')


class UnzipTar(luigi.Task):
  dataset = luigi.Parameter()
  data_row_path = luigi.Parameter(default='data_row')

  def run(self):
    file = tarfile.open(f'{self.data_row_path}/{self.dataset}.tar') 
    file.extractall(f'{self.data_row_path}/{self.dataset}')
    file.close()


class UnzipGz(luigi.Task):
  dataset = luigi.Parameter()
  data_row_path = luigi.Parameter(default='data_row')

  def run(self):
    for file in os.listdir(f'{self.data_row_path}/{self.dataset}'):
      file_path = os.path.join(f'{self.data_row_path}/{self.dataset}', file)

      if os.path.isfile(file_path):
        file_name = file.split('.')[0]
        os.makedirs(f'{self.data_row_path}/{self.dataset}/{file_name}', exist_ok=True)

        with gzip.open(file_path, 'rt') as file_input:
          file_data = file_input.read()

          with open(f'{self.data_row_path}/{self.dataset}/{file_name}/{file_name}.txt', 'w') as file_output:
            file_output.write(file_data)
            file_output.close()


if __name__ == '__main__':
  luigi.run()