using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Text;
using HtmlAgilityPack;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace ReadHandelsblatt
{
    public static class ReadHandelsblatt
    {
        [FunctionName("ReadHandelsblatt")]
        public static void Run([TimerTrigger("0 */10 * * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function execution saterted at: {DateTime.Now}");
            List<Newsarticle> ArticleList = ReadNews(log);
            WriteNews(log, ArticleList);
            log.LogInformation($"C# Timer trigger function execution completed at: {DateTime.Now}");
        }

        public static List<Newsarticle> ReadNews(ILogger log)
        {
            // generate list for articles
            List<Newsarticle> articleList = new List<Newsarticle>();

            // HANDELSBLATT GET ARTICLES
            string[] articleUrls = GetArticleUrls();

            // loop over all articles
            foreach (string articleUrl in articleUrls)
            {
                Newsarticle article = new Newsarticle();
                article.lang = "de";
                article.dataSite = "Handelsblatt";
                article.dataRegion = "DE";

                string Date = string.Empty;
                string Time = string.Empty;

                var url = articleUrl;
                var web = new HtmlWeb();
                var doc = web.Load(url);

                // gehe meta durch
                var list = doc.DocumentNode.SelectNodes("//meta");
                foreach (var node in list)
                {
                    if (node.GetAttributeValue("property", "").Contains("og:type"))
                    {
                        article.articleType = node.GetAttributeValue("content", "");
                    }
                    if (node.GetAttributeValue("property", "").Contains("og:title"))
                    {
                        article.articleHeadline = node.GetAttributeValue("content", "");
                    }
                    if (node.GetAttributeValue("name", "").Contains("description"))
                    {
                        article.articleSummary = node.GetAttributeValue("content", "");
                    }
                    if (node.GetAttributeValue("name", "").Contains("keywords"))
                    {
                        article.keywords = node.GetAttributeValue("content", "");
                        article.newsKeywords = node.GetAttributeValue("content", "");
                    }
                    if (node.GetAttributeValue("property", "").Contains("og:url"))
                    {
                        article.url = node.GetAttributeValue("content", "");
                    }
                }

                // gehe nun divs durch
                list = doc.DocumentNode.SelectNodes("//p");
                foreach (var node in list)
                {
                    if (node.OuterHtml.Contains("hb-date"))
                    {
                        Date = node.InnerText.Substring(0, 10);

                        article.articlePublished = node.InnerText;

                    }
                    if (node.OuterHtml.Contains("hb-time"))
                    {
                        Time = node.InnerText;
                    }

                    if (node.OuterHtml.Contains("hcf-location-mark"))
                    {
                        article.text = node.InnerText;
                        // first word is location
                        article.dataRegion = node.InnerText.Substring(0, node.InnerText.IndexOf(" "));
                        article.text = article.text.Substring(node.InnerText.IndexOf(" ") + 1);
                    }
                    if (node.OuterHtml.Contains("<p>") && node.OriginalName == "p"
                        && !node.InnerText.Contains("Schriftgröße")
                        && !node.InnerText.Contains("Mehr:")
                        && !node.InnerText.Contains("A +")
                        && !node.InnerText.Contains("a -"))
                    {
                        article.text += "\r\n" + node.InnerText.Replace("Ihr Browser unterstützt leider die Anzeige dieses Videos nicht.", "");
                    }
                }

                if (article.articlePublished.Contains("Uhr"))
                {
                    article.articlePublished = article.articlePublished.Replace(",", string.Empty).Replace(" Uhr", ":00");
                }
                else
                {
                    if ((!string.IsNullOrEmpty(Date)) && (!string.IsNullOrEmpty(Time)))
                    {
                        article.articlePublished = Date + " " + Time + ":00";
                    }
                    else
                    {
                        article.articlePublished = article.articlePublished + " 00:00:00";
                    }
                }

                // speichere die url zum bild
                list = doc.DocumentNode.SelectNodes("//img");
                foreach (var node in list)
                {
                    if (node.OuterHtml.Contains("hb-cImg"))
                    {
                        article.articleImageUrl = "https://app.handelsblatt.com/" + node.GetAttributeValue("data-src", "");
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

        public static string[] GetArticleUrls()
        {
            List<string> urls = new List<string>();

            var url = "https://app.handelsblatt.com/alle-schlagzeilen/";
            var web = new HtmlWeb();
            var doc = web.Load(url);

            var div = doc.DocumentNode.SelectSingleNode("//div[@class='hb-layout']");
            if (div != null)
            {
                var links = div.Descendants("a")
                               .Select(a => a.GetAttributeValue("href", ""))
                               .ToList();
                foreach (string link in links)
                {
                    urls.Add("https://app.handelsblatt.com/" + link);
                }
            }

            return urls.ToArray();
        }

        public static void WriteNews(ILogger log, List<Newsarticle> articleList)
        {

            var SqlConnectionString = Environment.GetEnvironmentVariable("string-sqldb-information");
            var DaysAgo = Environment.GetEnvironmentVariable("DaysAgo");
            var NewsAnalysisEndPoint = Environment.GetEnvironmentVariable("endpoint-newsanalysis");

            string sqlInput;
            StringBuilder sb;

            DateTimeOffset articlePublished;
            DateTimeOffset articleUpdated;

            DateTime thisTime = DateTime.Now;
            TimeZoneInfo tst = TimeZoneInfo.FindSystemTimeZoneById("W. Europe Standard Time");
            bool isDaylight = tst.IsDaylightSavingTime(thisTime);

            string DateTimeFormat = "dd.MM.yyyy HH:mm:ss zzz";

            List<string> NewsUrl = new List<string>();

            try
            {
                using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                {

                    sb = new StringBuilder();

                    sb.Append("SELECT DISTINCT [Url] FROM [News].[Values] WHERE [DataSite] = 'Handelsblatt' AND [RowCreated] > DATEADD(DAY,-" + DaysAgo.ToString() + ",SYSUTCDATETIME())");

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

                if (isDaylight)
                {
                    if (!string.IsNullOrEmpty(article.articlePublished))
                    {
                        article.articlePublished = article.articlePublished.Substring(0, Math.Min(20, article.articlePublished.Length));
                        article.articlePublished = article.articlePublished + " +02:00";
                    }
                }
                else
                {
                    if (!string.IsNullOrEmpty(article.articlePublished))
                    {
                        article.articlePublished = article.articlePublished.Substring(0, Math.Min(20, article.articlePublished.Length));
                        article.articlePublished = article.articlePublished + " +01:00";
                    }
                }

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

                                if (DateTimeOffset.TryParseExact(article.articlePublished, DateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out articlePublished))
                                {
                                    command.Parameters.AddWithValue("@ArticlePublished", new SqlDateTime(articlePublished.UtcDateTime));
                                    command.Parameters.AddWithValue("@ArticleUpdated", new SqlDateTime(articlePublished.UtcDateTime));
                                }
                                else
                                {
                                    command.Parameters.AddWithValue("@ArticlePublished", DateTime.UtcNow);
                                    command.Parameters.AddWithValue("@ArticleUpdated", DateTime.UtcNow);
                                }

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

                            sb.Append("SELECT [Guid],[ArticlePublished] FROM [News].[Values] WHERE [Url] = '" + article.url + "' AND[RowCreated] > DATEADD(DAY, -" + DaysAgo.ToString() + ", SYSUTCDATETIME())");

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

                                    if (DateTimeOffset.TryParseExact(article.articlePublished, DateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out articleUpdated))
                                    {
                                        command.Parameters.AddWithValue("@ArticlePublished", new SqlDateTime(ArticlePublished));
                                        command.Parameters.AddWithValue("@ArticleUpdated", new SqlDateTime(articleUpdated.UtcDateTime));
                                    }
                                    else
                                    {
                                        command.Parameters.AddWithValue("@ArticlePublished", ArticlePublished);
                                        command.Parameters.AddWithValue("@ArticleUpdated", ArticlePublished);
                                    }

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
