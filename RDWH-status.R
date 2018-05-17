if (!require("plyr")) install.packages("plyr", repos = "https://cloud.r-project.org/")
stopifnot(library(plyr, logical.return = TRUE))
if (!require("RODBC")) install.packages("RODBC", repos = "https://cloud.r-project.org/")
stopifnot(library(RODBC, logical.return = TRUE))
if (!require("mailR")) install.packages("mailR", repos = "https://cloud.r-project.org/")
stopifnot(library(mailR, logical.return = TRUE))
if (!require("htmlTable")) install.packages("htmlTable", repos = "https://cloud.r-project.org/")
stopifnot(library(htmlTable, logical.return = TRUE))

#------ Config ----------------------------------------------------------------------------------------------------------------- 
config <- data.frame(connection = c("Driver={SQL Server}; Server=192.168.10.64"), 
                     table = c("[Lulu_DWH].[dbo].[AUX_Adwords]", 
                               "[Lulu_DWH].[dbo].[AUX_Bing]", 
                               "[Lulu_DWH].[dbo].[AUX_GAnalytics]", 
                               "[Lulu_DWH].[dbo].[AUX_Upclick_AffiliateOverview]", 
                               "[Lulu_DWH].[dbo].[AUX_Upclick_Transactions]", 
                               "[SandBox].[dbo].[AUX_GAexperiments_data]", 
                               "[SandBox].[dbo].[AUX_PayPal]"), 
                     datecolumn = c("[Day]", 
                                    "[Date]", 
                                    "[Date]", 
                                    "[DayOfTheMonth]", 
                                    "[TransactionDate]",
                                    "[Date]",
                                    "[Date]"),
                     allowedLag = c(0, 
                                    0, 
                                    0, 
                                    0, 
                                    0, 
                                    1, 
                                    7), 
                     stringsAsFactors = FALSE)

conn <- odbcDriverConnect(config$connection[1])

report <- NULL
# loop through tables
for(i in 1:length(config$table)) {
  query <- paste0("SELECT MAX(", config$datecolumn[i], ") AS [MaxDate], MIN(", config$datecolumn[i], ") AS [MinDate]
                  FROM ", config$table[i])
  df <- sqlQuery(channel = conn, 
                 query = query, 
                 stringsAsFactors = FALSE)
  df$MaxDate <- as.Date(df$MaxDate)
  df$MinDate <- as.Date(df$MinDate)
  df$Recency <- paste0(Sys.Date() - df$MaxDate, " days")
  df$onSchedule <- Sys.Date() - df$MaxDate <= config$allowedLag[i]
  availableDates <- sqlQuery(channel = conn, 
                            query = paste0("SELECT DISTINCT ", config$datecolumn[i], " AS [Date] FROM ", config$table[i]), 
                            stringsAsFactors = FALSE)
  availableDates$Date <- as.Date(availableDates$Date)
  summary(availableDates$Date)
  dateSeq <- seq.Date(from = df$MinDate, to = Sys.Date() - config$allowedLag[i], by = "day")
  summary(dateSeq)
  missingDates <- dateSeq[!(dateSeq %in% availableDates$Date)]
  summary(missingDates)
  if (length(missingDates) > 0) {
    df$missingDates <- df$missingDates <- list(as.character(missingDates))
  } else {
    df$missingDates <- "none"
  }
  df$table <- config$table[i]
  report <- rbind.fill(report, df)
}

odbcCloseAll()

send.mail(from = "valeksandrov@lulusoftware.com", 
          to = c("bghereghloo@lulusoftware.com", 
                 "aabdallah@lulusoftware.com", 
                 "fgalarneau@lulusoftware.com", 
                 "valeksandrov@lulusoftware.com"), 
          subject = "DWH status report", 
          html = TRUE,
          body = paste0("<html><head></head><body>", htmlTable(report[, c("table", "onSchedule", "Recency", "missingDates")]), "</body></html>"),
          encoding = "utf-8", 
          smtp = list(host.name = "192.168.10.22", port = 25), # office environment
          authenticate = FALSE, 
          send = TRUE)