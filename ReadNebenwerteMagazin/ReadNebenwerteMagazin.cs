using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using HtmlAgilityPack;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace ReadNebenwerteMagazin
{
    public static class ReadNebenwerteMagazin
    {
        [FunctionName("ReadNebenwerteMagazin")]
        public static void Run([TimerTrigger("0 */10 * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function execution saterted at: {DateTime.Now}");
            List<Newsarticle> ArticleList = ReadNews(log);
            WriteNews(log, ArticleList);
            log.LogInformation($"C# Timer trigger function execution completed at: {DateTime.Now}");
        }

        public static List<Newsarticle> ReadNews(ILogger log)
        {
            // erstelle Liste für Artikel
            List<Newsarticle> articleList = new List<Newsarticle>();

            // ntv.de GET ARTICLES
            string[] articleUrls = getArticleUrls();

            // loop over all articles
            foreach (string articleUrl in articleUrls)
            {
                Newsarticle article = new Newsarticle();
                article.lang = "de";
                article.dataSite = "Nebenwerte Magazin";
                article.dataRegion = "DE";
                article.url = "https://www.nebenwerte-magazin.com" + articleUrl;
                article.articleSection = "Economics,business,Wirtschaft,Nebenwerte";

                if (article.url.Contains("tecdax"))
                    article.url += ",tecdax";
                if (article.url.Contains("mdax"))
                    article.url += ",tecdax";
                if (article.url.Contains("sdax"))
                    article.url += ",tecdax";


                var url = article.url;
                var web = new HtmlWeb();
                var doc = web.Load(url);


                // gehe meta durch

                var list = doc.DocumentNode.SelectNodes("//meta");
                foreach (var node in list)
                {
                    if (node.GetAttributeValue("name", "") == "author")
                    {
                        article.author = node.GetAttributeValue("content", "");
                    }
                    if (node.GetAttributeValue("name", "") == "description")
                    {
                        article.articleSummary = node.GetAttributeValue("content", "");
                    }
                    if (node.GetAttributeValue("name", "") == "keywords")
                    {
                        article.keywords = node.GetAttributeValue("content", "");
                    }
                    if (node.GetAttributeValue("name", "") == "keywords")
                    {
                        article.newsKeywords = node.GetAttributeValue("content", "");
                    }
                    if (node.GetAttributeValue("name", "") == "date")
                    {
                        article.articlePublished = DateTime.Now.ToString();
                    }
                    if (node.GetAttributeValue("name", "") == "modified")
                    {
                        article.articleUpdated = node.GetAttributeValue("content", "");
                    }
                    if (node.GetAttributeValue("property", "") == "og:image")
                    {
                        article.articleImageUrl = node.GetAttributeValue("content", "");
                    }
                    if (node.GetAttributeValue("name", "") == "title")
                    {
                        article.articleHeadline = node.GetAttributeValue("content", "");
                    }
                }


                var div = doc.DocumentNode.SelectSingleNode("//div[@class='itemFullText']");
                if (div != null)
                {
                    article.text = div.InnerText.Replace("\r\n", "<br />");

                    string bild = div.OuterHtml;
                    int index = bild.IndexOf("src=");
                    if (index != -1)
                    {
                        bild = bild.Substring(index + 5);
                        index = bild.IndexOf("\"");
                        article.articleImageUrl = "https://www.nebenwerte-magazin.com" + bild.Substring(0, index);
                    }
                }

                if (!string.IsNullOrEmpty(article.text))
                {
                    article.articleWordCount = article.text.Length;
                }
                if (!string.IsNullOrEmpty(article.articleImageUrl))
                {
                    article.articleImageCount = 1;
                }

                articleList.Add(article);
            }

            return articleList;
        }

        public static string[] getArticleUrls()
        {
            List<string> urls = new List<string>();

            var url = "https://www.nebenwerte-magazin.com";
            var web = new HtmlWeb();
            var doc = web.Load(url);



            var div = doc.DocumentNode.SelectSingleNode("//div[@class='k2ItemsBlock']");
            if (div != null)
            {
                var links = div.Descendants("a")
                               .Select(a => a.GetAttributeValue("href", ""))
                               .ToList();
                foreach (string link in links)
                {
                    urls.Add(link);
                }
            }

            return urls.Distinct().ToArray();
        }

        public static void WriteNews(ILogger log, List<Newsarticle> articleList)
        {

            var SqlConnectionString = Environment.GetEnvironmentVariable("string-sqldb-information");
            var DaysAgo = Environment.GetEnvironmentVariable("DaysAgo");
            var NewsAnalysisEndPoint = Environment.GetEnvironmentVariable("endpoint-newsanalysis");

            string sqlInput;
            StringBuilder sb;

            List<string> NewsUrl = new List<string>();

            try
            {
                using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                {

                    sb = new StringBuilder();

                    sb.Append("SELECT DISTINCT [Url] FROM [News].[Values] WHERE [DataSite] = 'Nebenwerte Magazin' AND [RowCreated] > DATEADD(DAY,-" + DaysAgo.ToString() + ",SYSUTCDATETIME())");

                    sqlInput = sb.ToString();


                    using (SqlCommand command = new SqlCommand(sqlInput, connection))
                    {
                        connection.Open();
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                NewsUrl.Add(reader.GetString(0));
                            }
                        }
                        connection.Close();

                    }
                }

            }
            catch (SqlException ex)
            {
                log.LogInformation(ex.ToString());
            }

            foreach (Newsarticle article in articleList)
            {
                if (!NewsUrl.Contains(article.url))
                {
                    Guid NewsGuid = Guid.NewGuid();

                    try
                    {
                        using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                        {
                            sb = new StringBuilder();

                            sb.Append(@"INSERT INTO [News].[Values]([Guid],[Lang],[DataSite],[DataRegion],[ArticleType],[ArticleSection],[ArticleTag],[ArticleHeadline],[ArticleSummary],[ArticlePublished],[ArticleUpdated],[Keywords],[NewsKeywords],[Author],[ArticleWordCount],[ArticleImageCount],[Url],[ArticleImageUrl],[ArticleOpinion],[Text],[TextPaywall])
                                            VALUES (@Guid,@Lang,@DataSite,@DataRegion,@ArticleType,@ArticleSection,@ArticleTag,@ArticleHeadline,@ArticleSummary,@ArticlePublished,@ArticleUpdated,@Keywords,@NewsKeywords,@Author,@ArticleWordCount,@ArticleImageCount,@Url,@ArticleImageUrl,@ArticleOpinion,@Text,@TextPaywall)");

                            sqlInput = sb.ToString();

                            using (SqlCommand command = new SqlCommand(sqlInput, connection))
                            {
                                connection.Open();

                                command.CommandType = CommandType.Text;
                                command.Parameters.Clear();

                                command.Parameters.AddWithValue("@Guid", new SqlGuid(NewsGuid));
                                command.Parameters.AddWithValue("@Lang", new SqlString(article.lang));
                                command.Parameters.AddWithValue("@DataSite", new SqlString(article.dataSite));
                                command.Parameters.AddWithValue("@DataRegion", new SqlString(article.dataRegion));
                                command.Parameters.AddWithValue("@ArticleType", new SqlString(article.articleType));
                                command.Parameters.AddWithValue("@ArticleSection", new SqlString(article.articleSection));
                                command.Parameters.AddWithValue("@ArticleTag", new SqlString(article.articleTag));
                                command.Parameters.AddWithValue("@ArticleHeadline", new SqlString(article.articleHeadline));
                                command.Parameters.AddWithValue("@ArticleSummary", new SqlString(article.articleSummary));

                                command.Parameters.AddWithValue("@ArticlePublished", new SqlDateTime(DateTime.UtcNow));
                                command.Parameters.AddWithValue("@ArticleUpdated", new SqlDateTime(DateTime.UtcNow));

                                command.Parameters.AddWithValue("@Keywords", new SqlString(article.keywords));
                                command.Parameters.AddWithValue("@NewsKeywords", new SqlString(article.newsKeywords));
                                command.Parameters.AddWithValue("@Author", new SqlString(article.author));
                                command.Parameters.AddWithValue("@ArticleWordCount", new SqlInt32(article.articleWordCount));
                                command.Parameters.AddWithValue("@ArticleImageCount", new SqlInt32(article.articleImageCount));
                                command.Parameters.AddWithValue("@Url", new SqlString(article.url));
                                command.Parameters.AddWithValue("@ArticleImageUrl", new SqlString(article.articleImageUrl));
                                command.Parameters.AddWithValue("@ArticleOpinion", new SqlString(article.articleOpinion));
                                command.Parameters.AddWithValue("@Text", new SqlString(article.text));
                                command.Parameters.AddWithValue("@TextPaywall", new SqlString(article.textPaywall));

                                command.ExecuteReader();

                                connection.Close();

                            }
                        }

                        System.Net.WebClient wc = new System.Net.WebClient();
                        wc.DownloadString(NewsAnalysisEndPoint + "guid=" + NewsGuid.ToString("D"));

                    }
                    catch (SqlException ex)
                    {
                        log.LogInformation(ex.ToString());
                    }
                }
                else
                {
                    Guid Guid = Guid.Empty;
                    DateTime ArticlePublished = DateTime.UtcNow;

                    try
                    {
                        using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                        {
                            sb = new StringBuilder();

                            sb.Append("SELECT [Guid],[ArticlePublished] FROM [News].[Values] WHERE [Url] = '" + article.url + "' AND [RowCreated] > DATEADD(DAY,-" + DaysAgo.ToString() + ",SYSUTCDATETIME())");

                            sqlInput = sb.ToString();

                            using (SqlCommand command = new SqlCommand(sqlInput, connection))
                            {
                                connection.Open();
                                using (SqlDataReader reader = command.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        Guid = reader.GetGuid(0);
                                        ArticlePublished = reader.GetDateTime(1);
                                    }
                                }
                                connection.Close();

                            }
                        }
                    }
                    catch (SqlException ex)
                    {
                        log.LogInformation(ex.ToString());
                    }

                    if (Guid != Guid.Empty)
                    {
                        try
                        {
                            using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                            {
                                sb = new StringBuilder();

                                sb.Append($@"UPDATE [News].[Values] SET
                                                [Lang] = @Lang,
                                                [DataSite] = @DataSite,
                                                [DataRegion] = @DataRegion,
                                                [ArticleType] = @ArticleType,
                                                [ArticleSection] = @ArticleSection,
                                                [ArticleTag] = @ArticleTag,
                                                [ArticleHeadline] = @ArticleHeadline,
                                                [ArticleSummary] = @ArticleSummary,
                                                [ArticlePublished] = @ArticlePublished,
                                                [ArticleUpdated] = @ArticleUpdated,
                                                [Keywords] = @Keywords,
                                                [NewsKeywords] = @NewsKeywords,
                                                [Author] = @Author,
                                                [ArticleWordCount] = @ArticleWordCount,
                                                [ArticleImageCount] = @ArticleImageCount,
                                                [Url] = @Url,
                                                [ArticleImageUrl] = @ArticleImageUrl,
                                                [ArticleOpinion] = @ArticleOpinion,
                                                [Text] = @Text,
                                                [TextPaywall] = @TextPaywall
                                            WHERE [Guid] = '{Guid.ToString("D")}'
                                            ");

                                sqlInput = sb.ToString();

                                using (SqlCommand command = new SqlCommand(sqlInput, connection))
                                {
                                    connection.Open();

                                    command.CommandType = CommandType.Text;
                                    command.Parameters.Clear();

                                    command.Parameters.AddWithValue("@Lang", new SqlString(article.lang));
                                    command.Parameters.AddWithValue("@DataSite", new SqlString(article.dataSite));
                                    command.Parameters.AddWithValue("@DataRegion", new SqlString(article.dataRegion));
                                    command.Parameters.AddWithValue("@ArticleType", new SqlString(article.articleType));
                                    command.Parameters.AddWithValue("@ArticleSection", new SqlString(article.articleSection));
                                    command.Parameters.AddWithValue("@ArticleTag", new SqlString(article.articleTag));
                                    command.Parameters.AddWithValue("@ArticleHeadline", new SqlString(article.articleHeadline));
                                    command.Parameters.AddWithValue("@ArticleSummary", new SqlString(article.articleSummary));

                                    command.Parameters.AddWithValue("@ArticlePublished", new SqlDateTime(ArticlePublished));
                                    command.Parameters.AddWithValue("@ArticleUpdated", new SqlDateTime(DateTime.UtcNow));

                                    command.Parameters.AddWithValue("@Keywords", new SqlString(article.keywords));
                                    command.Parameters.AddWithValue("@NewsKeywords", new SqlString(article.newsKeywords));
                                    command.Parameters.AddWithValue("@Author", new SqlString(article.author));
                                    command.Parameters.AddWithValue("@ArticleWordCount", new SqlInt32(article.articleWordCount));
                                    command.Parameters.AddWithValue("@ArticleImageCount", new SqlInt32(article.articleImageCount));
                                    command.Parameters.AddWithValue("@Url", new SqlString(article.url));
                                    command.Parameters.AddWithValue("@ArticleImageUrl", new SqlString(article.articleImageUrl));
                                    command.Parameters.AddWithValue("@ArticleOpinion", new SqlString(article.articleOpinion));
                                    command.Parameters.AddWithValue("@Text", new SqlString(article.text));
                                    command.Parameters.AddWithValue("@TextPaywall", new SqlString(article.textPaywall));

                                    command.ExecuteReader();

                                    connection.Close();
                                }
                            }
                        }
                        catch (SqlException ex)
                        {
                            log.LogInformation(ex.ToString());
                        }
                    }


                }
            }

        }

        public class Newsarticle
        {
            public string lang { get; set; }                    // Language                         e.g. EN
            public string dataSite { get; set; }                // Data Source                      e.g. NYT
            public string dataRegion { get; set; }              // Which Region                     e.g. New York
            public string articleType { get; set; }             // Type of Article                  e.g. Article
            public string articleSection { get; set; }          // Section                          e.g. Business
            public string articleTag { get; set; }              // Article Tags                     e.g. United States Economy
            public string articleHeadline { get; set; }        // Headline                        
            public string articleSummary { get; set; }          // Summary of Article
            public string articlePublished { get; set; }        // Published Date                   e.g. 2020-03-09T23:09:20.000Z
            public string articleUpdated { get; set; }          // Last Modified                    e.g. 2020-03-10T05:21:56.000Z
            public string keywords { get; set; }                // Article keywords                 e.g. US Economy,Banking and Finance,Coronavirus,Stocks;Bonds
            public string newsKeywords { get; set; }            // News keywords                    e.g. US Economy,Banking and Finance,Coronavirus,Stocks;Bonds
            public string author { get; set; }                  // Article Author                   e.g. Name of author
            public int articleWordCount { get; set; }           // #Words                           
            public int articleImageCount { get; set; }       // #Images
            public string url { get; set; }                     // URL of article                   
            public string articleImageUrl { get; set; }         // URL to images                    
            public string articleOpinion { get; set; }          // Is Article opinion?              e.g. Yes or No
            public string text { get; set; }                    // Article full text
            public string textPaywall { get; set; }             // Article first paragraph

            public Newsarticle()
            {
                string lang = string.Empty;
                string dataSite = string.Empty;
                string dataRegion = string.Empty;
                string articleType = string.Empty;
                string articleSection = string.Empty;
                string articleTag = string.Empty;
                string articleHeadline = string.Empty;
                string articleSummary = string.Empty;
                string articlePublished = string.Empty;
                string articleUpdated = string.Empty;
                string newsKeywords = string.Empty;
                string author = string.Empty;
                int articleWordCount = 0;
                int articleImageCount = 0;
                string url = string.Empty;
                string articleImageUrl = string.Empty;
                string articleOpinion = string.Empty;
                string text = string.Empty;
                string textPaywall = string.Empty;

            }
        }
    }
}
