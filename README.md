# Spark-Sftp
Spark-Sftp Using Pipeline

Ex:

 

Suppose following are the jobs in your project-

 

Job1 -> reading data from hdfs.

Job2 -> applying some transformation on data

Job3 -> send modified data to sftp location

----

 

Add jobs in pipeline and start.(parallel or sequential it’s on developers call)

 

Pipeline >> Job1 >> Job2 >> Job3  start

=======================================================================================
//use AsyncExecution to switch to parallel execution
  implicit val pipelineExecutor: PipelineExecutor[Seq[String], Some[String]] = SyncExecution()
  
  //way to add stages in pipeline
  //change read/write format ex: MultiFileReader(Formatter.apply("parquet")), default format is csv
  def createPipline =
   val pl = Pipeline[Seq[String], Seq[String]]() >>
      MultiFileReader(Formatter.apply()) >>
      MultiFileWriter(Formatter.apply()) >>
      SftpMultiFileWriter()  
      
   pl.start
