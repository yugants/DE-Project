import traceback
import datetime

class S3Reader:


    def __init__(self, s3_client, bucket_name, logger):
        
        self.s3_client= s3_client
        self.logger = logger
        self.bucket = bucket_name


    def list_files(self, folder_path):
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket,Prefix=folder_path)
            if 'Contents' in response:
                self.logger.info("Total files available in folder '%s' of bucket '%s': %s", folder_path, self.bucket, response)
                files = [f"s3://{self.bucket}/{obj['Key']}" for obj in response['Contents'] if
                         not obj['Key'].endswith('/')]
                return files
            else:
                return []
        except Exception as e:
            error_message = f"Error listing files: {e}"
            traceback_message = traceback.format_exc()
            self.logger.error("Got this error : %s",error_message)
            print(traceback_message)
            raise
    
    def download_files(self, local_directory, list_files):
        
        self.logger.info("Running download files for these files %s",list_files)
        for key in list_files:
            file_name = os.path.basename(key)
            self.logger.info("File name %s ",file_name)
            download_file_path = os.path.join(local_directory, file_name)
            try:
                self.s3_client.download_file(self.bucket,key,download_file_path)
            except Exception as e:
                error_message = f"Error downloading file '{key}': {str(e)}"
                traceback_message = traceback.format_exc()
                print(error_message)
                print(traceback_message)
                raise e
            

    def upload_to_s3(self,s3_directory,local_file_path):
        current_epoch = int(datetime.datetime.now().timestamp()) * 1000
        s3_prefix = f"{s3_directory}/{current_epoch}/"
        try:
            for root, dirs, files in os.walk(local_file_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    s3_key = f"{s3_prefix}/{file}"
                    self.s3_client.upload_file(local_file_path, self.bucket, s3_key)
            return f"Data Successfully uploaded in {s3_directory} data mart "
        except Exception as e:
            self.logger.error(f"Error uploading file : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e

################### Directory will also be available if you use this ###########

    # def list_files(self, bucket):
    #     try:
    #         response = self.s3_client.list_objects_v2(Bucket=self.bucket)
    #         if 'Contents' in response:
    #             files = [f"s3://{self.bucket}/{obj['Key']}" for obj in response['Contents']]
    #             return files
    #         else:
    #             return []
    #     except Exception as e:
    #         print(f"Error listing files: {e}")
    #         return []