#Caminhos diretórios:
dir_log <- "C:\Users\Arthur\Documents\CEFET\Pratica_de_Pesquisa\trabalho\log"

set_log <- function()
{
  tryCatch({
    
    #set log
    if(!dir.exists('/tmp/log'))
      dir.create('/tmp/log')  
    
    logFile = paste("C:\Users\Arthur\Documents\CEFET\Pratica_de_Pesquisa\trabalho\log", paste(as.character(Sys.time(), '%Y%m%d_%H%M%S'), 'log_json2rdata.txt', sep = '_'), sep = '/')
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), "Iniciando processo ... ", sep = ' - '), file=logFile, append=TRUE, sep = "")
    
  }, error = function(err) {
    
    print(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste("Erro ao tentar criar arquivo de log. Msg erro: ", err), sep = ' - '))
    stop()
    
  })
}

set_log()


#filtros
dat_ini <- '2017-01-01'
qtd_dias_processar <- 1

#repositorios
str_url_download <- 'http://aldebaran.eic.cefet-rj.br/data/mobility/zip/'

if(!dir.exists('/tmp/repositorio_json_cru'))
  dir.create('/tmp/repositorio_json_cru')  

dir_json <- '/tmp/repositorio_json_cru'

if(!dir.exists('/tmp/repositorio_zip_cru'))
  dir.create('/tmp/repositorio_zip_cru')

dir_zip <- '/tmp/repositorio_zip_cru'

if(!dir.exists('/shared/mobility/repositorio_r_data_cru'))
  dir.create('/shared/mobility/repositorio_r_data_cru')

dir_rdata <- '/shared/mobility/repositorio_r_data_cru'

#funcoes
cria_repositorio_dia <- function(dia)
{
  tryCatch({
    
    if(!dir.exists(paste('/tmp/repositorio_json_cru', dia, sep = "/")))
      dir.create(paste('/tmp/repositorio_json_cru', dia, sep = "/"))  
    
  }, error = function(err) {
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste("ERRO_EM_cria_repositorio_dia:  ",err), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    return(FALSE)
    
  })
  
}

download_zip <- function(url_origem, dir_destino)
{
  tryCatch({
    
    download.file(url_origem, dir_destino)
    
  }, error = function(err) {
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste("ERRO_EM_DESCOMPACTA_ZIP:  ",err), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    return(FALSE)
    
  })
  
}

descompcta_zip <- function(arq_zip, dir_destino)
{
  tryCatch({
    
    if (file.exists(arq_zip)){
      unzip(arq_zip, exdir = dir_destino)
      #file.remove(arq_zip)
    } else {
      cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste("Arquivo nÃ£o encontrado", basename(arq_zip), sep = " "), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    }
    
  }, error = function(err) {
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste("ERRO_EM_DOWNLOAD:  ",err), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    return(FALSE)
  })
  
}

formata_colunas <- function(x)
{
  tryCatch({
    
    x$V1 <- sub("(.{2}).(.{2}).(.{4})", '\\3-\\2-\\1', x$V1)
    
    x$V2 <- as.character(x$V2)
    
    x$V3 <- as.character(x$V3)
    
    x$V4 <- as.numeric(as.character(x$V4))
    
    x$V5 <- as.numeric(as.character(x$V5))
    
    x$V6 <- as.numeric(as.character(x$V6))
    
    return (x);
  }, error = function(err) {
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste("ERRO_EM_formata_colunas:  ",err), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    return(FALSE)
    
  })
}

renomeia_colunas <- function(tbl)
{
  tryCatch({
    
    names(tbl)[1]='DATAHORA'
    names(tbl)[2]='ORDEM'
    names(tbl)[3]='LINHA'
    names(tbl)[4]='LATITUDE'
    names(tbl)[5]='LONGITUDE'
    names(tbl)[6]='VELOCIDADE'
    
    return (tbl);
  }, error = function(err) {
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste("ERRO_EM_renomeia_colunas:  ",err), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    return(FALSE)
    
  })
}

processa_dia <- function(dat)
{
  
  tryCatch({
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste('Processa: ', dat), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    url_com_data_do_arquivo <- paste(dat, '.zip', sep = '')
    
    url_file <- paste(str_url_download, url_com_data_do_arquivo, sep='')
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste('Inicia download de', dat, sep = ' '), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    nom_arquivo_zip <- paste(dat, '.zip', sep='')
    
    download_zip(url_file, paste(dir_zip, nom_arquivo_zip, sep='/'))
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste('Fim download de', dat, sep = ' '), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste('Inicia unzip de', dat, sep = ' '), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    descompcta_zip(paste(dir_zip, nom_arquivo_zip, sep='/'), paste(dir_json, dat, sep = "/"))
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste('Fim unzip de', dat, sep = ' ') , sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    #tem arquivos zipados que os .json estÃ£o na pasta tmp
    lis_json_files<- list.files(paste(paste(dir_json, dat, sep = "/"), "tmp", sep="/"),
                                
                                pattern="\\.json",
                                
                                all.files=FALSE,
                                
                                full.names=TRUE)
    
    #e tem arquivos zipados que os .json estÃ£o na raiz
    if(length(lis_json_files) == 0){
      lis_json_files<- list.files(paste(dir_json, dat, sep = "/"),
                                  
                                  pattern="\\.json",
                                  
                                  all.files=FALSE,
                                  
                                  full.names=TRUE)
    }  
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste(paste('Qtd arquivos .JSON: ', length(lis_json_files)), dat, sep = ' | '), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste('Inicia processo de conversao de .JSON para R Data de', dat, sep = ' '), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    i <- 0
    indice <- 1
    lis_df_aux <- list(data.frame())
    
    for (json_file in lis_json_files){
      
      cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste('Processa arquivo ', paste(dat, basename(json_file), sep = ' ')), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
      
      tryCatch({
        
        if(json_file != '')
        {
          library("jsonlite")
          json_result <-  fromJSON(json_file, simplifyDataFrame = FALSE)
          
          json_df <- data.frame()
          
          json_df <- as.data.frame(json_result[[2]])
          
          json_df <- formata_colunas(json_df)
          
          if(i < 100){
            lis_df_aux[[indice]] <- rbind(lis_df_aux[[indice]], json_df)
            i <- i + 1
          }else{
            i <- 0
            indice <- indice + 1
            lis_df_aux[[indice]]<- list(data.frame())
            lis_df_aux[[indice]] <- rbind(lis_df_aux[[indice]], json_df)
          }
          
          file.remove(json_file)
          
        }
      }, error = function(err) {
        
        cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste("ERRO_EM_JSON2RDATA:  ",err), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
        
        return(FALSE)
      })
    }
    
    df_full <- data.frame()
    i <- 1
    for(df in lis_df_aux){
      cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste(i, paste("o df_auxliar adicionado", dat, sep = ' | '), sep = ''), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
      df_full <- rbind(df_full, df)
      i <- i + 1  
    }
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), 'Fim processo de conversao', sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    nom_file = paste(dir_rdata,paste(dat, '.Rda', sep = ''), sep = '/')
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste('nom_file: ', nom_file), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    df_full <- renomeia_colunas(df_full)
    
    save(df_full,  file = nom_file)
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste('Fim processo do dia ', dat), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
  }, error = function(err) {
    
    cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), paste("ERRO_EM_PROCESSA_DIA:  ",err), sep = ' - '), file=logFile, append=TRUE, sep = "\n")
    
    return(FALSE)
  })
  
}


library(SparkR)

sparkR.session(appName = "json2rdata")

dias <- format(seq(as.POSIXct(dat_ini), by = "day", length.out = qtd_dias_processar), "%Y-%m-%d")

spark.lapply(dias, processa_dia)

cat(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), 'Fim processamento!', sep = ' - '), file=logFile, append=TRUE, sep = "\n")

sparkR.session.stop()

'
tryCatch({
  set_log()
  print("teste")  
}, error = function(err) {
  
  print(paste(format(Sys.time(), "%Y/%m/%d %H:%M:%S"), "Erro na execução do script", sep = ' - '))
  stop()
  
})
'





