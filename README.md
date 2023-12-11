# 📊 Read Financial News

This is a set of different timer triggers that run as Azure functions and read different financial new portals and stored the respective informaiton in a database. While performing the read, the text from the news gets send to be analyzed by an NLP model deployed as a HTTP trigger which is also an Azure function.

## ⚙️ Functionality
ßß0juz54efgbn    
The process is fairly similar for all the portals. Web scraping from the different sources occurs to get a list with the differnt news.

## 📜 Notes

This was part of a larger project that never went into production, so a cleaner implementation with a more TDD approach won't happen.

The project is being made public without the git history.