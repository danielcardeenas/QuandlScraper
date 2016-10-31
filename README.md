# Quandl Scraper
Downloads Quandl Data (Bulks) without a premium account and import it to a PostgreSQL (+ 9.2) database

###Dependecies
+ [Npgsql](http://www.npgsql.org/install.html)
+ [JSON.NET](http://www.newtonsoft.com/json)

###Setup
+ In class: [``QuandlScraper/TecEnergyQuandl/Utils/Constants.cs``](https://github.com/danielcardeenas/QuandlScraper/blob/master/TecEnergyQuandl/Utils/Constants.cs)
  + Replace ```API_KEY``` with your Quandl api key
  + Replace ```USER``` with your Postgres user
  + Replace ```PASSWORD``` with your Postgres password
  
_Thats it. The program will handle by itself the creation of database/schemas/tables etc.._
  
###Usage
The program consists in 3 key tasks.
Each one is independient from each other. So you can skip a task (commenting a line of code) and the program will still run fine.

1. [**Fetch Databases** available] (https://github.com/danielcardeenas/QuandlScraper/blob/master/TecEnergyQuandl/Program.cs#L28)
2. [**Fetch Datasets** available] (https://github.com/danielcardeenas/QuandlScraper/blob/master/TecEnergyQuandl/Program.cs#L28) <sub>(from selected databases downloaded in step 1)</sub>
3. [**Fetch Data**] (https://github.com/danielcardeenas/QuandlScraper/blob/master/TecEnergyQuandl/Program.cs#L28) <sub>(from selected databases downloaded in step 2)</sub>
