import os
# import io
import boto3
import pandas as pd

class S3:


    def __init__(self, logger, bucket_name = 'de-retail-bucket'):

        print('hello')
        
        df = pd.read_csv('C:\\Users\\yugant.shekhar\\OneDrive - Blue Altair\\Desktop\\Douments\\AWS\\temp.csv')

        aws_access_key = df['Access key ID'][0]
        aws_secret_key = df['Secret access key'][0]
        
        self.s3 = boto3.resource(
            service_name = 's3'
            , region_name = 'eu-north-1'
            , aws_access_key_id=aws_access_key
            , aws_secret_access_key=aws_secret_key
        )
        self.logger = logger
        self.bucket = bucket_name

        print('Bucket: ', self.bucket)

        print('Initialized logger')


    
    def get_real_file(self, local_file_path, format):

        '''
        Get parquet from
        the folder structure 
        '''
        
        try:
            
            # List all files in the folder
            file_names = os.listdir(local_file_path)

            # Optionally, filter out only files (ignore directories)
            file_names = [f for f in file_names if os.path.isfile(os.path.join(local_file_path, f))]

            # print('Dirs: ',file_names)

            for i in file_names:

                # print(i.split('.')[-1])
                if format == 'parquet' and 'parquet' == i.split('.')[-1]:
                    original_file = i
                    print('original file: ', original_file)
                    return original_file
                
                elif format == 'csv' and 'csv' == i.split('.')[-1]:
                    original_file = i
                    print('original file: ', original_file)
                    return original_file
                
        except Exception as e:

            self.logger.exception(e)


    def upload_to_s3(self, s3_directory, local_file_path, format):

        '''
        For uploading files
        to S3 bucket
        '''
            
        # self.logger.info('Writing Parquet file to S3')
        file = self.get_real_file(local_file_path, format)

        # print('Returned Path: ', file)
        # print('Returned Type: ', type(file))
        # print('Local file Type: ', type(local_file_path))
        local_file_path = local_file_path + file
        # print('Final path: ', local_file_path)
        
        try:
            (self.s3.Bucket(self.bucket)
            .upload_file(Filename=local_file_path
                        ,Key=s3_directory))
            
            self.logger.info(f'File written at {s3_directory} from {local_file_path}')

        except Exception as e:

            self.logger.exception(e)


    # def list_files(self, folder_path):
    #   pass
    

    # def read_file(self, spark, s3_location, format):

    #     try:
    #         print('In read')
    #         print('Bucket: ', self.bucket)
    #         print('----------------------')
            
    #         obj = (self.s3.Bucket(self.bucket)
    #                 .Object(s3_location).get())

    #         buffer = io.BytesIO(obj['Body'].read())

    #         if format == 'parquet':
    #             df_pd = pd.read_parquet(buffer)
    #             print('Pandas: ', df_pd)
            
    #         elif format == 'csv':
    #             df_pd = pd.read_csv(buffer)


    #         df = spark.createDataFrame(df_pd)


    #         self.logger.info(f'Read from {s3_location}')
    #         self.logger.info(df.show())
    #         return df

    #     except Exception as e:

    #         self.logger.exception(e)            
        
