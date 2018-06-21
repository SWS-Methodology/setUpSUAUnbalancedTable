##' # Pull data from different domains to sua 
##'
##' **Author: Cristina Muschitiello** modified by cldb
##' Modifications: 
##' -only pulls data for unvalidated countries
##' -pulls data for opening stocks, live animals, 
##' 
##'
##' **Description:**
##'
##' This module is designed to harvest the data from other tables and pull all
##' relevant FBS data into the SUA/FBS domain.  It pulls from the following
##' 
##' **Inputs:**
##'
##' * Agriculture Production (production, stock, seed, industrial)
##' * Food (food)
##' * Loss (loss)
##' * feed (feed) 
##' * stock (stock) 
##' * Trade:
##' in november 2017, for urgent purposes, as it was not possible to validate all the new Trade data
##' it has been decided to use:
##'    . Old Trade data up to 2013
##'    . New Trade data from 2014 (Trade domain)
##' * Tourist (tourist)

##'
##' **Flag assignment:**
##'
##' | Observation Status Flag | Method Flag|
##' | --- | --- | --- |


## load the library
library(faosws)
library(data.table)
library(faoswsUtil)
library(sendmailR)
library(dplyr)

################################################
#####       set environment    #####
################################################ 

R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
  message("Not on server, so setting up environment...")
  
  library(faoswsModules)
  SETT <- ReadSettings("pullDataToSUA_2013/sws.yml")
  #SETT<- AddSettings(dir = "setUpSUAUnbalancedTable/pullDataToSUA_2013", filename = "sws.yml", gitignore = T,
  #                   fields = NULL)
  
  R_SWS_SHARE_PATH <- SETT[["share"]]  
  ## Get SWS Parameters
  SetClientFiles(dir = SETT[["certdir"]])
  GetTestEnvironment(
    baseUrl = SETT[["server"]],
    token = SETT[["token"]]
  )
}


################################################
#####       get sua data                   #####
################################################
yearkey = 2013
geoKey = GetCodeList(domain = "suafbs", dataset = "sua_unbalanced", "geographicAreaM49")[,code]
itemKey = GetCodeList(domain = "suafbs", dataset = "sua_unbalanced", "measuredItemFbsSua")[,code]
elementKey = GetCodeList(domain = "suafbs", dataset = "sua_unbalanced", "measuredElementSuaFbs")[,code]


# sessionKey = swsContext.datasets[[1]]

geoDim = Dimension(name = "geographicAreaM49", keys = geoKey)

eleDim = Dimension(name = "measuredElementSuaFbs", keys = elementKey)

itemDim = Dimension(name = "measuredItemFbsSua", keys = itemKey)

timeDim = Dimension(name = "timePointYears", keys = as.character(yearkey))


suaKey = DatasetKey(domain = "suafbs", dataset = "sua_unbalanced",
                    dimensions = list(
                      geographicAreaM49 = geoDim,
                      measuredElement = eleDim,
                      measuredItemCPC = itemDim,
                      timePointYears = timeDim)
)

suaData = GetData(suaKey)


stockvar13<- suaData %>% filter(measuredElementSuaFbs == 5071 & timePointYears==2013 & Value!=0)
stockop13<-stockvar13
stockop13[,"measuredElementSuaFbs"]<-"5113"
stockop13[,"Value"]<-0

################################################
#####        Harvest from stockdata        #####
################################################

message("Pulling data from Stock domain")
stocksCode = c("5113")

stockEleDim = Dimension(name = "measuredElement",
                        keys = stocksCode)
stockitemKeys = GetCodeList(domain = "agriculture", dataset = "aproduction",
                       dimension = "measuredItemCPC")[, code]
itemDim = Dimension(name = "measuredItemCPC", keys = stockitemKeys)

stokKey = DatasetKey(domain = "Stock", dataset = "stocksdata",
                     dimensions = list(
                       geographicAreaM49 = geoDim,
                       measuredElement = stockEleDim,
                       measuredItemCPC = itemDim,
                       timePointYears = Dimension(name = "timePointYears", keys = as.character(2014:2016)))
)
stockData = GetData(stokKey)
setnames(stockData, c("measuredElement", "measuredItemCPC"),
         c("measuredElementSuaFbs", "measuredItemFbsSua"))





###########

################################################
#####       Merging data files together    #####
################################################

message("Merging data files together and saving")
out = rbind(stockop13,stockData)
#protected data
#### CRISTINA: after havig discovered that for crops , official food values are Wrong and have to be deleted. 
# now we have to delete all the wrong values:
# THE FOLLOWING STEPS HAVE BEEN COMMENTED BECAUSE THEY SHOULD NOT BE NEEDED
# the data might have to be corrected from the questionnaires


#### The previous step has been inserted here and removed from the standardization in order
# to give to the data team the possibility to eventually add some food value for primary commodities


stats = SaveData(domain = "suafbs", dataset = "sua_unbalanced", data = as.data.table(out), waitTimeout = 2000000)

paste0(stats$inserted, " observations written, ",
       stats$ignored, " weren't updated, ",
       stats$discarded, " had problems.")


################################################################
#####  send Email with notification of correct execution   #####
################################################################

## Initiate email
from = "sws@fao.org"
to = swsContext.userEmail
subject = "PullDataToSua plug-in has correctly run"
body = "The plug-in has saved the SUAs in your session"

sendmail(from = from, to = to, subject = subject, msg = body)
paste0("Email sent to ", swsContext.userEmail)

