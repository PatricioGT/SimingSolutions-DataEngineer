from py7zr import is_7zfile,SevenZipFile
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import  #lit,udf,lenght
from pyspark.sql.types import 
import time

spark = SparkSession 
.builder 
.appName(App to load csv data from smtp SSolution path) 
.getOrCreate()

ACCESS_KEY = str(sys.argv)[2]
SECRET_KEY = str(sys.argv)[3]

startTime = time.time()

class LoadConsumtions    
    
    def __init__(self,localPath,localPathNew)
        self.localPath = localPath
        self.localPathNew = localPathNew
    
    def checkFileCorrupted(self,path7Z)
        return is_7zfile(path7Z)
        
    def checkFirstColumn(self,df1)
        df2 = df1.select().where(length(df1._c0)!=22)
        return(len(df2.head(1)) == 0)
        
    def checkFileExtension(self,file,file_ext)
        file_name,file_extension = os.path.splitext(file)
        if file_extension == file_ext
            return True
        return False

    def getFileInfo(self,file)
        return os.path.splitext(file)
    
    def run(self)
        #Extract 7z
        for file in os.listdir(self.localPath)    
            path7Z = self.localPath+file
            if self.checkFileCorrupted(path7Z)
                with SevenZipFile(path7Z, 'r') as zipFile
                     allFiles = zipFile.getnames()
                     for f in allFiles
                        if self.checkFileExtension(f,'.0')
                            fileName,fileExtension = self.getFileInfo(f)
                            zipFile.extract(localPathNew+fileName)
            else
                print(Check 1 - Corrupted compression - excluded file +file)
        print (--- Extract 7z %s seconds --- % (time.time() - startTime))
                            
        #Read and create parquet
        for fileName in os.listdir(self.localPathNew)
            csvFilePath = self.localPathNew+fileName+''+fileName+'.0'
            parquetFilePath = self.localPathNew+fileName+'.parquet'    
            dfConsumtions = spark.read.csv(csvFilePath,header=False, sep=';')           
            if self.checkFirstColumn(dfConsumtions)
                dfConsumtions2 = dfConsumtions.withColumn('file_name',lit(fileName))
                dfConsumtions2.write.parquet(parquetFilePath,mode='overwrite')
            else
                print(Check 2 - Length of firt column - excluded file +fileName)
        print (--- Read and create parquet %s seconds --- % (time.time() - startTime))
                
                    
        #Upload to S3
        s3_client = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)
        bucket = 'consumos-ss'
        folder = 'consumos'       
        for fileName in os.listdir(self.localPathNew)
            if self.checkFileExtension(fileName,'.parquet')
                parquetFilePath2 = self.localPathNew+fileName+''
                for fileNameParquet in os.listdir(parquetFilePath2)
                    if self.checkFileExtension(fileNameParquet,'.parquet')  
                        finalLocalPath = parquetFilePath2+fileNameParquet
                        folderObject = folder+fileNameParquet
                        try
                            response = s3_client.upload_file(finalLocalPath,bucket,folderObject)
                        except ClientError as e
                            logging.error(e)  
        print (--- Upload to S3 %s seconds --- % (time.time() - startTime))               
        
        return (--- %s seconds --- % (time.time() - startTime))
    
localPath = 'CUserspatriDocumentsSSolutionsloadNewFile'
localPathNew = 'CUserspatriDocumentsSSolutionsloadNewFilenewFiles'
WorkLoad = LoadConsumtions(localPath,localPathNew)
result = WorkLoad.run()
print(result)
                    
