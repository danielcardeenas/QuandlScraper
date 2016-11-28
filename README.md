# Quandl Scraper
Downloads Quandl Data (Bulks) without a premium account and import it to a PostgreSQL (+ 9.2) database.

Everything gets _structured N I C E L Y_

###Dependecies
+ [Npgsql](http://www.npgsql.org/install.html)
+ [JSON.NET](http://www.newtonsoft.com/json)

###Setup
+ In class: [`QuandlScraper/TecEnergyQuandl/Utils/Constants.cs`](https://github.com/danielcardeenas/QuandlScraper/blob/master/TecEnergyQuandl/Utils/Constants.cs)
  + Replace `API_KEY` with your Quandl api key
  + Replace `USER` with your Postgres user
  + Replace `PASSWORD` with your Postgres password
  
_Thats it. The program will handle by itself the creation of database/schemas/tables etc.._

###Usage
1. Program will download all databases available and insert them into **quandl.databases** (in a database named quandl in the postgres instance) and then pause
  + Here you have to select the databases in postgres by setting `import` to `true`. Example: 
  ```sql
  update quandl.databases set import = true where databasecode = 'WIKI';
  ```
2. Program will download all datasets available from `WIKI` and istert them into **quandl.datasets** and then pause
  + Here you have to select the datasets in postgres by setting `import` to `true`. Example:
  ```sql
  update quandl.datasets set import = true where databasecode = 'WIKI' and datasetcode = 'AAPL';
  update quandl.datasets set import = true where databasecode = 'WIKI' and datasetcode = 'FB';
  ```
3. Program will download all the data available from `WIKI/AAPL` and `WIKI/FB`, dynamically creates table **quandl.wiki** and inserts all the data

####**(Extra)** Usage for datatables
1. Program will insert all datatables available and insert them into **quandl.datatables** (in a database named quandl in the postgres instance) and then pause
  + Here you have to select the datatables in postgres by setting `import` to `true`. Example: 
  ```sql
  update quandl.datatables set import = true where name = 'ZACKS/CP';
  ```
  
2. Program will download all the data available from `ZACKS/CP`, dynamically creates table **ZACKS/CP** and inserts all the data

###Info
The program consists in 3 key tasks (4 if you count datatables fetch).
Each one is independient from each other. So you can skip a task (commenting the code line) and the program will still run fine.

1. [Fetch Databases available] (https://github.com/danielcardeenas/QuandlScraper/blob/master/TecEnergyQuandl/Program.cs#L28)
2. [Fetch Datasets available] (https://github.com/danielcardeenas/QuandlScraper/blob/master/TecEnergyQuandl/Program.cs#L32) <sub>(from selected databases downloaded in step 1)</sub>
3. [Fetch Data] (https://github.com/danielcardeenas/QuandlScraper/blob/master/TecEnergyQuandl/Program.cs#L37) <sub>(from selected databases downloaded in step 2)</sub>
4. [Fetch Datatables] (https://github.com/danielcardeenas/QuandlScraper/blob/master/TecEnergyQuandl/Program.cs#L40) **(Extra)** <sub>(from selected datatables downloaded)</sub>

If something unexpected happens a file `log.txt` (inside same directory where the excutable is running) will be created with the information, however, the program will do his best to handle the error and keep working.
