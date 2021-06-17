

### Jerry Shan
### Last updated: 03/24/2021, 5/11/2021


# install.packages("fastDummies") 
# library(fastDummies)

# getwd()
# setwd("~/Documents/Projects/Wisdom")
# rm(list=ls())

new.raw.data.extraction.s<-function(){

  my.file<-"presto-faro-obfuscated_run_3_stmt_1_0.csv"
  Raw.df0<-read.csv(my.file, head=T)
  
  Raw.df<-df.col.name.replace.s(Raw.df0, 
                          c("datepartition",       "gso_geo_region",      "gso_lms_vertical",     "gso_tier_summary",    "enablelan",          
                                "country_name",        "crm_sub_vertical",    "is_autobidding",      "cost_type",            "objective_type",     
                                 "budget",              "revenue_recognized",  "revenue_total",       "impressions",         "impressions_onsite", 
                                "impressions_offsite",  "clicks",              "clicks_onsite",        "clicks_offsite",      "revenue_onsite",     
                                "revenue_offsite",      "number_of_campaigns"),
                          c("Date", "Region", "Vertical", "Tier",  "LAN",
                                "Country",        "sub_vertical",    "Autobidding",      "Cost",            "Obj",     
                                "budget",              "rev_recog",  "rev_total",       "imprs",         "imprs_ons", 
                                "imprs_offs",  "clicks",              "clicks_ons",        "clicks_offs",      "rev_ons",     
                                "rev_offs",      "nb_of_campaigns"))
  
  Raw.df$Autobidding<-as.factor(as.logical(Raw.df$Autobidding))
  
  #Raw.df<-df.col.name.replace.s(Raw.df0, "datepartition",  "date")
  
  tmp<-substring(Raw.df$Date, 1, 10) 
  Raw.df$Date<-paste(substring(tmp, 1, 4), substring(tmp, 6, 7), substring(tmp, 9, 10), sep='')
  Raw.df
  
  # seems "20200801" thru "20201101".
}



config.sort.by.a.metric.s<-function(Raw.df, metrics.to.sort="rev_total"){
  if(missing(Raw.df)){
    Raw.df<-new.raw.data.extraction.s()
  }
  
  dimensions<-c("Vertical", "Country",  "Tier")  
  dimensions.plural<-c(c("Verticals", "Countries",  "Tiers"))
  all.levels.in.all.dims<-vector("list", length(dimensions))
  
  my.Config<-rep("", length(dimensions))
  names(all.levels.in.all.dims)<-names(my.Config)<-dimensions
  
  
  for(i in 1:length(dimensions)){
    this.dim<-dimensions[i]
    agg.df<-aggregate(Raw.df[, metrics.to.sort], by=list(Raw.df[, this.dim]), sum) 
    sorted.agg.df<-df.sort.s(agg.df, sort.by.col=2, decreasing=T)
    all.levels.in.all.dims[[i]]<-sorted.agg.df[, 1]
  }
  
  for(i in 1:length(dimensions)){
    this.dim<-dimensions[i]
    cat.s("Pick index value(s) from the following list: (default with return key is for selecting all) \n")
    all.levels<-all.levels.in.all.dims[[i]]
    print(all.levels)
    ix<-scan(what=0)
    if(length(ix)==0) {
      ix<-1:length(all.levels)
      my.Config[i]<-paste("All.",  dimensions.plural[i], sep="")
      
    }
    else if(length(ix)>1){
      my.Config[i]<-paste(all.levels[ix], collapse="+")
    }
    else { # length(ix)==1
      my.Config[i]<- all.levels[ix]
    }
  }
  my.Configs.txt<-paste(my.Config, collapse = "-")
  my.Configs.txt
  
}



list.to.txt.s<-function(x.list=sample.Config1){
  
  nb.components<-length(x.list)
  connected.txt<-rep("", nb.components)
  for(i in 1:nb.components){
    this.component.nmae<-names(x.list)[i]
    this.coponent.elements<-x.list[[i]]
    this.coponent.elements.expressed<-paste(this.coponent.elements, collapse=" + ")
    connected.txt[i]<-paste("{", this.component.nmae, ": ", this.coponent.elements.expressed, "}", sep="")
  }
  paste(connected.txt, collapse = " x ")
}


# RAW.df0<-new.raw.data.extraction.s()
# RAW.df0$autobidding<-as.factor(as.logical(RAW.df0$autobidding))


#sample.group1<-"Tech-United States-ENT"
#sample.group2<-"All.Vertical-All.Country-All.Tier"

group.name.split.s<-function(x=sample.group2){
  
  Components<-unlist(strsplit(x, split="-"))
  names(Components)<-c("Vertical", "Country", "Tier")
  Components
  
}

data.filtering.s<-function(Raw.df=RAW.df0, Config=sample.group2){
  

  if(missing(Raw.df)){
    Raw.df<-new.raw.data.extraction.s()
  }
  if(missing(Config)){
    Config<-config.sort.by.a.metric.s(Raw.df=Raw.df, metrics.to.sort="rev_total")
  }
 
  
  Components<-group.name.split.s(x=Config)
  my.vertical<-unlist(strsplit(Components[1], split="[+]"))
  my.country<-unlist(strsplit(Components[2], split="[+]"))
  my.tier<-unlist(strsplit(Components[3], split="[+]"))
  my.selections<-list(Vertical=my.vertical, Country=my.country, Tier=my.tier)
  
  if(length(my.vertical) ==1 && substring(my.vertical, 1, 3)  =="All")
    Vertical.Condition<-rep(T, length(Raw.df$Vertical))
  else
    Vertical.Condition<- is.element(Raw.df$Vertical, my.vertical)
  
  if(length(my.country) ==1 && substring(my.country, 1, 3)=="All") 
    Country.Condition<-rep(T, length(Raw.df$Country))
  else
    Country.Condition<-is.element(Raw.df$Country,  my.country)
  
  if(length(my.tier) ==1 && substring(my.tier, 1, 3)=="All")
    Tier.Condition<-rep(T, length(Raw.df$Tier))
  else
    Tier.Condition<-is.element(Raw.df$Tier, my.tier) 
  
  selection<-  Country.Condition & Vertical.Condition & Tier.Condition
  data.df<-Raw.df1<-Raw.df[selection,   ]
  # config.columns<-c("Vertical", "Country", "Tier")
  
  # Raw.df2<-Raw.df1 #   df.names.cols.remove.s(Raw.df1, config.columns) 
  # Raw.df3<-aggregate(.~ Date, Raw.df2, sum)
  #data.df<-df.sort.s(Raw.df3, sort.by.col=1, decreasing=F)
  data.df
  # Monthly.ts<-Monthly.df.to.ts.s(data.df)
  # Monthly.df<-ts.to.df.s(Monthly.ts, month.column=T)
  # names(Monthly.df)<-c("Month", "Budget")
  # write.csv(Monthly.df, file="Monthly.csv", row.names = F)
  # list(Config=Config, Monthly.TS=Monthly.ts)
  list(Config = Config, Data=data.df, Selection=my.selections)
}



j.plot.design.s<- function (x, y = NULL, fun = mean, data = NULL, ..., ylim = NULL, 
          xlab = "", ylab = NULL, main = NULL, ask = NULL, xaxt = par("xaxt"), 
          axes = TRUE, xtick = FALSE, Graph=T) 
{
  .plot.des <- function(x, y, fun, ylab, ylim = NULL, Graph=Graph, ...) {
    if (!is.numeric(y)) 
      stop("'y' must be a numeric vector")
    if (!is.data.frame(x)) 
      stop("'x' must be a data frame")
    if (!all(sapply(x, is.factor)) & !is.factor(x)) 
      stop("all columns/components of 'x' must be factors")
    k <- ncol(x)
    if (anyNA(y)) {
      FUN <- fun
      fun <- function(u) FUN(u[!is.na(u)])
    }
    tot <- fun(y)
    stats <- lapply(x, function(xc) tapply(y, xc, fun))
    variability.s<-function(x){
      max(x)-min(x)
    }
    variability.s<-sd
    out<-lapply(stats, variability.s)
    
    if (any(is.na(unlist(stats)))) 
      warning("some levels of the factors are empty", call. = FALSE)
    if (is.null(ylim)) 
      ylim <- range(c(sapply(stats, range, na.rm = TRUE), 
                      tot))
  
    if(Graph){
      plot(c(0, k + 1), ylim, type = "n", axes = axes, xaxt = "n", 
           xlab = xlab, ylab = ylab, main = main, adj = 0.5, 
           ...)
      grid(nx=NULL)
      abline(v=1:k, lty=3, col="gray")
      segments(0.5, tot, k + 0.5, tot, ...)
      for (i in 1L:k) {
        si <- stats[[i]]
        segments(i, min(si, na.rm = TRUE), i, max(si, na.rm = TRUE), 
                 ...)
        for (j in 1L:(length(si))) {
          sij <- si[j]
          segments(i - 0.05, sij, i + 0.05, sij, ...)
          text(i - 0.1, sij, labels = names(sij), adj = 1, 
               ...)
        }
      }
     
      if (axes && xaxt != "n") {}
        if(F) {
          axis(1, at = 1L:k, names(stats), xaxt = xaxt, tick = xtick,    
             mgp = {
               p <- par("mgp")
               c(p[1L], if (xtick) p[2L] else 0, 0)
              },  ...)
         # mtext(at = (1L:k)[seq(from=1L, by=2, to= length(1L:k))], text=names(stats)[seq(from=1L, by=2, to= length(1L:k))], srt=-45, cex=0.6, side=1)
         # mtext(at = (1L:k)[seq(from=1L+1, by=2, to= length(1L:k))], text=names(stats)[seq(from=1L+1, by=2, to= length(1L:k))], srt=-45, cex=0.6, side=3)
        }
    
        delta<-(par("usr")[4]-par("usr")[3])/60
        text(x=(1L:k), y=par("usr")[3]- delta, labels= paste(names(stats), " ", sep=""),  cex=0.5, srt=45, xpd=T, adj=1) 
        }  
    out
  }
  fname <- deparse(substitute(fun))
  fun <- match.fun(fun)
  if (!(is.data.frame(x) | inherits(x, "formula"))) 
    stop("'x' must be a dataframe or a formula")
  if (is.data.frame(x)) {
    if (is.null(y)) {
    }
    else if (inherits(y, "formula")) {
      x <- stats::model.frame(y, data = x)
    }
    else if (is.numeric(y)) {
      x <- cbind(y, x[, sapply(x, is.factor)])
      tmpname <- match.call()
      names(x) <- as.character(c(tmpname[[3L]], names(x[, 
                                                        -1])))
    }
    else if (is.character(y)) {
      ynames <- y
      y <- data.frame(x[, y])
      if (sum(sapply(y, is.numeric)) != ncol(y)) {
        stop("a variable in 'y' is not numeric")
      }
      x <- x[, sapply(x, is.factor)]
      xnames <- names(x)
      x <- cbind(x, y)
      names(x) <- c(xnames, ynames)
    }
  }
  else if (is.data.frame(data)) {
    x <- stats::model.frame(x, data = data)
  }
  else {
    x <- stats::model.frame(x)
  }
  i.fac <- sapply(x, is.factor)
  i.num <- sapply(x, is.numeric)
  nResp <- sum(i.num)
  if (nResp == 0) 
    stop("there must be at least one numeric variable!")
  yname <- names(x)[i.num]
  if (is.null(ylab)) 
    ylab <- paste(fname, "of", yname)
  ydata <- as.matrix(x[, i.num])
  if (!any(i.fac)) {
    x <- data.frame(Intercept = rep.int(" ", nrow(x)))
    i.fac <- 1
  }
  xf <- x[, i.fac, drop = FALSE]
  if (is.null(ask)) 
    ask <- prod(par("mfcol")) < nResp && dev.interactive(orNone = TRUE)
  if (ask) {
    oask <- devAskNewPage(ask)
    on.exit(devAskNewPage(oask))
  }
  for (j in 1L:nResp) OUT<-.plot.des(xf, ydata[, j], fun = fun, srt=0,
                                     ylab = ylab[j], ylim = ylim, Graph=Graph, ...)
  
  OUT
}


vector.to.intervals.s<-function(x=c(1.123, 2.213, 3.312), digits=2){
  n<-length(x)
  y<-round(x, d=digits)
  out<-vector("character", n+1)
  for(i in 1:(n+1)){
    if(i==1)  out[i]<-paste("<=", y[i])
    else if (i==n+1) out[i]<-paste(">", y[i-1])
    else out[i]<-paste("(", y[i-1], ", ",  y[i], "]", sep="")
  }
  out
}

analysis.s<-function(Raw.df0=RAW.df0, 
                     Response=c("revenue_recognized", "revenue_total",  "impressions",  "impressions_onsite",
                     "impressions_offsite", "clicks", "clicks_onsite",  "clicks_offsite",
                     "revenue_onsite",  "revenue_offsite")[1]){
   graphics.off()
  
  
   if(missing(Raw.df0)) Raw.df0<-new.raw.data.extraction.s()
   Raw.list<-data.filtering.s(Raw.df=Raw.df0)
   Raw.df<-Raw.list$Data
   Config<-Raw.list$Config
   Selection<-Raw.list$Selection
  

   Disc.Factors<-c("Autob", "Cost",  "Obj", "LAN")
   Dimensions<-c("Vertical", "Country", "Tier")
   
   for(i in 1:length(Dimensions)){
       this.dim<-Dimensions[i]
       nb.elements<-length(Selection[[this.dim]])
       if( nb.elements > 1 || (nb.elements ==1 && substring(Selection[[this.dim]], 1, 3)=="All"))
  
         Disc.Factors<-c(Disc.Factors, this.dim) 
   }   
     
   # Disc.Factors<-c(Disc.Factors, "Country", "Vertical", "Tier")
   
   Disc.X.df<-Raw.df[, Disc.Factors]
   
   Raw.df[, "budget.in.K"]<-Raw.df[, "budget"]/10^3
   Num.Variables<-c("budget.in.K", "nb_of_campaigns")
   Num.X.df<-Raw.df[, Num.Variables]
   Num.X.factor.df<-apply(Num.X.df, 2, discretize2.s, digits=2)
  
   X.disc.df<-cbind(Disc.X.df, Num.X.factor.df)
   X.disc.df<-Disc.X.df
   
   for(i in 1:dim(X.disc.df)[2]){
     X.disc.df[, i]<-as.factor(X.disc.df[, i])
   }
   
   
   merged.dat.df<-cbind(Raw.df[, Response], X.disc.df)
   names(merged.dat.df)[1]<-Response
   
   driver.list<-  setdiff(names(merged.dat.df), Response)
   my.formula.1 <- as.formula(paste(Response, " ~ ", paste(driver.list, collapse= "+")))
   
   
   PD<-j.plot.design.s(my.formula.1, data=merged.dat.df, Graph=F, cex=0.5)
   sorted.variables<-names(sort(unlist(PD), decreasing=T))
   
   my.formula.sorted <- as.formula(paste(Response, " ~ ", paste(sorted.variables, collapse= "+")))
   my.main<-paste("Factor Importance and Level Quantification on ", Response, " for ", Config, sep="")
   PD<-j.plot.design.s(my.formula.sorted, data=merged.dat.df, Graph=T, cex=0.3, cex.axis=0.3, main =my.main)
   
   
   if(RF.Model<-T){
       library("randomForest")
   
      browser()
       model.rf<-randomForest(my.formula.1, data= merged.dat.df)
       model.rf$importance
      dev.new(width=16, height=10)
      par(mfrow=c(1, 1))
      my.main<-paste("Variable Importance Ranking", "w.r.t.", Response, "for",  Config, sep=" " )
      varImpPlot(model.rf, main= my.main)
   }
   PD
  
}


discretize2.s<-function(x=rnorm(100), digits=3){
  probabilities<-c(0.01, 0.25, 0.5,  0.75, 0.99)
  # probabilities<-c(0, 0.5, 1)
  my.q<-quantile(x, prob=probabilities, na.rm=T)
  index<-findInterval(x, my.q)
  my.labels<-vector.to.intervals.s(my.q, d=digits)
  my.labels[index+1]
}


#install.packages("dplyr")          
#library("dplyr")     

factor.level.count.s<-function(x=as.factor(c("A", "B", "A", "C"))){
  
   #data.vec <- data.frame(x)  
   #freq.df<-dplyr::count(data_vec, vec)  
   #freq.df
   
   summary(as.factor(x))
  
  
}

data.daily.agg.s<-function(Data.df=RAW.df0, Config=""){
 

    if(M<-missing(Data.df)) {
      Raw.df0<-new.raw.data.extraction.s()
      Raw.list<-data.filtering.s(Raw.df=Raw.df0)
      Data.df<-Raw.list$Data
      Config<-Raw.list$Config
      Selection<-Raw.list$Selection
    }
  
    Disc.Factors<-c("Autobidding", "Cost",  "Obj", "LAN")
    Dimensions<-c("Vertical", "Country", "Tier")
    
    if(M){
       for(i in 1:length(Dimensions)){
          this.dim<-Dimensions[i]
          nb.elements<-length(Selection[[this.dim]])
          if( nb.elements > 1 || (nb.elements ==1 && substring(Selection[[this.dim]], 1, 3)=="All"))
              Disc.Factors<-c(Disc.Factors, this.dim)  
       } 
    }
    else{
      Disc.Factors<- c(Disc.Factors, Dimensions)
    }
    
    Num.Columns<-c("budget",  "rev_recog", "rev_total",  "imprs",
                   "imprs_ons",  "imprs_offs",  "clicks",  "clicks_ons", 
                   "clicks_offs",  "rev_ons",  "rev_offs",  "nb_of_campaigns")
    
    K<-dim(Data.df)[1] # 30
    Num.X.df<-Data.df[,c("Date", Num.Columns)]  
    Agg.Num.X.df<-aggregate(Num.X.df[, -1], by=list(Date= Num.X.df$Date), sum)[1:K, ]
    
    all.dates<-sort(unique(Data.df$Date))
 
    for(d in (1:length(all.dates))){
      this.date<- all.dates[d]
      this.Data.df<-subset(Data.df, Date==this.date)
      Disc.X.df<-this.Data.df[, Disc.Factors]
      Num.X.df<-this.Data.df[,c("Date", Num.Columns)]
      summation<-numeric(0)
      for(i in 1:length(Disc.Factors)){
           this.factor<-Disc.Factors[i]
           this.column<-as.factor(Disc.X.df[, this.factor])
           this.fac.summation<-summary(this.column, maxsum = length(unique(this.column)))
           # the default in summary for S3 is 100. 
           levels(this.column)[levels(this.column)==""]<-"NA"
           names(this.fac.summation)<-paste(this.factor, ".", levels(this.column), sep="")
           summation<-c(summation, this.fac.summation)
      }
      
      tmp<-cbind(Date=this.date, as.data.frame(t(summation)))
      if(d==1) Agg.Disc.X.df<-tmp
      else Agg.Disc.X.df<-merge(Agg.Disc.X.df, tmp, all=T)
        #  rbind(Agg.Disc.X.df, tmp) works if all days have the same levels. 
    }
   
    daily.df<- merge(Agg.Num.X.df, Agg.Disc.X.df)
    daily.df.names<-colnames(daily.df)
    daily.df$utilization<-daily.df$rev_recog/daily.df$budget
    daily.df<-daily.df[, c("Date", "utilization",  daily.df.names[-1])]
    
    list(Config=Config, Data=daily.df)
}


data.dummized.s<-function(Data.df=RAW.df0, Config=""){
  
  
  if(M<-missing(Data.df)) {
    Raw.df0<-new.raw.data.extraction.s()
    Raw.list<-data.filtering.s(Raw.df=Raw.df0)
    Data.df<-Raw.list$Data
    Config<-Raw.list$Config
    Selection<-Raw.list$Selection
  }
  
  Disc.Factors<-c("Autobidding", "Cost",  "Obj", "LAN")
  Dimensions<-c("Vertical", "Country", "Tier")
  
  if(M){
    for(i in 1:length(Dimensions)){
      this.dim<-Dimensions[i]
      nb.elements<-length(Selection[[this.dim]])
      if( nb.elements > 1 || (nb.elements ==1 && substring(Selection[[this.dim]], 1, 3)=="All"))
        Disc.Factors<-c(Disc.Factors, this.dim)  
    } 
  }
  else{
    Disc.Factors<- c(Disc.Factors, Dimensions)
  }
  
  Data.df$utilization<-Data.df$rev_recog/Data.df$budget
  
  Num.Columns<-c("budget",  "rev_recog", "rev_total",  "imprs",
                 "imprs_ons",  "imprs_offs",  "clicks",  "clicks_ons", 
                 "clicks_offs",  "rev_ons",  "rev_offs",  "nb_of_campaigns", "utiliuzation")
  
  Data.df$Autobidding<-as.factor(Data.df$Autobidding)
  Data.df1<-Data.df[1:10, ]
  Data.df2<-df.names.cols.remove.s(Data.df1, c("Date", "sub_vertical"))
  
 
  Data.df3<-dummy_cols(Data.df2)
  
  Data.df4<-cbind(Date=Data.df1$Date, Data.df3)
 
  
  list(Config=Config, Data=Data.df4)
}

is.numeric.s<-function(dat.df){
  nb.cols<-dim(RAW.df)[2]
  out<-logical(nb.cols)
  for (i in 1:nb.cols){
    this.col<-dat.df[, i]
    out[i]<-is.numeric(this.col)
  }
  out
}

combined.dummied.s<-function(Data.df0=RAW.df0[1:10, ], Config="", Chosen.Continuous.Vars=c("budget", "rev_recog")[1:2], 
                             Chosen.Dummized.Vars=c("autobidding", "Obj")[1]) {
  
  # Data.df0$autobidding<-as.factor(as.logical(Data.df0$autobidding))
  
  Dat.df1<-Data.df0[, c(Chosen.Continuous.Vars, Chosen.Dummized.Vars)]
  Dat.Dummized.df<-dummy_cols(Dat.df1)
  
  # All.Continuous.Vars<-colnames(Data.df0)[is.numeric.s(Data.df0)]
  
  all.dummized.vars<-setdiff(colnames(Dat.Dummized.df),  c(Chosen.Continuous.Vars, Chosen.Dummized.Vars))
  
  combined.dummized.names<-character(0)
  for(i in 1:length(Chosen.Continuous.Vars)){
     this.chosen.continuous.var<-Chosen.Continuous.Vars[i]
     tmp<-paste(this.chosen.continuous.var, ".", all.dummized.vars, sep="")
     combined.dummized.names<-c(combined.dummized.names, tmp)
  }
  
  Dat.Combined.Dummized.df<-Dat.Dummized.df
  
  for(i in 1:length(combined.dummized.names)){
     this.name<-combined.dummized.names[i]
     subs<-unlist(strsplit(this.name, "\\."))
     if(length(subs)==2) {
        this.name.value<-Dat.Dummized.df[, subs[1]]* Dat.Dummized.df[, subs[2]]
        Dat.Combined.Dummized.df<-cbind(Dat.Combined.Dummized.df, this.name.value)
        nb.cols<-dim(Dat.Combined.Dummized.df)[2]
        colnames(Dat.Combined.Dummized.df)[nb.cols]<-this.name
     }
  }
  
  data.frame(Date=Data.df0$Date, df.names.cols.remove.s(Dat.Combined.Dummized.df, all.dummized.vars))
}

data.daily.agg.from.dummies.s<-function(Data.df0=RAW.df0, Chosen.Continuous.Vars=c("budget", "rev_recog")[1],
                                        Chosen.Dummized.Vars=c("Autobidding", "Obj")[1]){
  # browser()
 
  Combined.Dummied.df<-combined.dummied.s(Data.df0=Data.df0, Config="", Chosen.Continuous.Vars=Chosen.Continuous.Vars, 
                               Chosen.Dummized.Vars=Chosen.Dummized.Vars)
  
  #response.vars<-setdiff(colnames(Combined.Dummied.df), c("Date", Chosen.Continuous.Vars, Chosen.Dummized.Vars))
  
  response.vars<-setdiff(colnames(Combined.Dummied.df), c("Date", Chosen.Dummized.Vars))
  
  
  my.formula.txt<-paste("cbind(", paste(response.vars, collapse=","), ") ~ Date", sep="") 
  my.formula<-as.formula(my.formula.txt)
  
  daily.Combined.Dummied.df<-aggregate(my.formula, data=Combined.Dummied.df, FUN=sum)
  
  daily.Combined.Dummied.df
  
}

daily.data.analysis.s<-function(Daily.df=Daily.df0, Config="", Response="rev_recog"){ 
  graphics.off()
 
 
  driver.list<-names(Daily.df)[2:19]
  
  Num.X.df<-Daily.df[, driver.list]
    
  Num.X.factor.df<-apply(Num.X.df, 2, discretize2.s, digits=2)
  
  X.disc.df<- Num.X.factor.df
  
  merged.dat.df<-data.frame(Daily.df[, Response], X.disc.df)
  names(merged.dat.df)[1]<-Response
  
  for(i in 2:dim(merged.dat.df)[2]){
    merged.dat.df[, i]<-as.factor(merged.dat.df[, i])
  }
  
  driver.list<-  setdiff(names(merged.dat.df), Response)
  my.formula.1 <- as.formula(paste(Response, " ~ ", paste(driver.list, collapse= "+")))
  
  
  PD<-j.plot.design.s(my.formula.1, data=merged.dat.df, Graph=F, cex=0.5)
  sorted.variables<-names(sort(unlist(PD), decreasing=T))
  
  my.formula.sorted <- as.formula(paste(Response, " ~ ", paste(sorted.variables, collapse= "+")))
  my.main<-paste("Factor Importance and Level Quantification on ", Response, " for ", Config, sep="")
  PD<-j.plot.design.s(my.formula.sorted, data=merged.dat.df, Graph=T, cex=0.5, cex.axis=0.5, main =my.main)
  
 #################
  
  
  library("randomForest")
  
  my.formula.2<-as.formula(paste(Response, " ~ ", paste(driver.list, collapse= "+")))
  model.rf<-randomForest(my.formula.2, data=  Daily.df)
  model.rf$importance
  dev.new(width=16, height=10)
  par(mfrow=c(1, 1))
  my.main<-paste("Variable Importance Ranking", "w.r.t.", Response, "for",  Config, sep=" " )
 
  out<-varImpPlot(model.rf, main= my.main)
  out
}

daily.plot.s<-function(Daily.df= Daily.df0, Config="", Metric="Autob.0"){
    
     # all.metrics<-names(Daily.Data.df)[-1]

     all.metrics<-c("Autob.0",                 "Autob.1",                 "Cost.CPC",                "Cost.CPM",                "Cost.CPV",               
                  "Obj.NA",                  "Obj.BRAND_AWARENESS",     "Obj.CREATIVE_ENGAGEMENT", "Obj.ENGAGEMENT" ,         "Obj.JOB_APPLICANT",      
                  "Obj.LEAD_GENERATION",     "Obj.TALENT_LEAD",         "Obj.VIDEO_VIEW",          "Obj.WEBSITE_CONVERSION",  "Obj.WEBSITE_TRAFFIC",    
                  "Obj.WEBSITE_VISIT",       "LAN.false",               "LAN.true",                "budget",                  "rev_recog",              
                  "rev_total",               "imprs",                   "imprs_ons",               "imprs_offs",              "clicks",                 
                  "clicks_ons",              "clicks_offs",             "rev_ons",                 "rev_offs",                "nb_of_campaigns",        
                  "utilization")          
    
     Dat.df0<-Daily.df[, c("Date", Metric)]
     Dat.df0$Acct.Month<-substring(Dat.df0$Date, 1, 6)
     Dat.df0<-Dat.df0[, c("Acct.Month", "Date",Metric)]
     names(Dat.df0)<-c("Acct.Month", "Date", "Quantity")

     my.main<-paste("Daily Observations of", Metric, "for", Config)
     j.daily.in.months.generic.plot.s(Dat.Df=Dat.df0, my.main=my.main)
     Dat.df0
}


#
#RAW.df0<-new.raw.data.extraction.s()

analysis.steps.s<-function(RAW.df=RAW.df0){
  #
  # Step 1. complete raw data extraction
 
  graphics.off()
  
  if(F){
     rm(list=ls())
     source('~/Documents/Projects/Wisdom/Features.R')
     source('~/Documents/Projects/Wisdom/targeted.utility.R')
     source('~/Documents/Projects/Wisdom/generic.utility.R')
     source('~/Documents/Ads Business Forecasting/data.aggregation2.R')
     RAW.df0<-new.raw.data.extraction.s()
  }                                   

  
  if(missing(RAW.df)) RAW.df<-new.raw.data.extraction.s()
  
  # Step 2. filtering by segment

  Selected.Raw.list<-data.filtering.s(Raw.df=RAW.df0)
  
  # Step 3. Data in selected segment, based on the interactive segment selection 
  
  Selected.Raw.df<-Selected.Raw.list$Data
  Selected.Config<-Selected.Raw.list$Config
  
  # Step 4. Aggregation at daily level
  
  Daily.Data.list0<-data.daily.agg.s(Data.df=Selected.Raw.df, Config=Selected.Config)
  Daily.df0<- Daily.Data.list0$Data
  this.Config<-Daily.Data.list0$Config
  
  # Dat.df0<-Daily.Data.list0$Data
  # Visualizing
  
  this.Metric<- c("Autobidding.0",  "Obj.CREATIVE_ENGAGEMENT")[2]
  daily.plot.s(Daily.df=Daily.df0, Config=this.Config, Metric=this.Metric)
  pause.s()
  
  # Step 5. Analysis on the aggregated daily data: identification and ranking of important factor and levels.
  
  daily.data.analysis.s(Daily.df=Daily.df0, Config=this.Config, Response="utilization")
  pause.s()
  daily.data.analysis.s(Daily.df=Daily.df0, Config=this.Config, Response="utilization")
  
  # the average budget utilization in days when the daily count of campaign objective being "creative engagement" is in a specified bucket. 
  paired.data.df<-Daily.df0[, c("Date",  "utilization", "Obj.CREATIVE_ENGAGEMENT")]
  
  # j.daily.in.months.generic.plot.s()
  

 #  daily.plot.s(Daily.df=Daily.df0, Metric="Obj.CREATIVE_ENGAGEMENT")
 

 # j.daily.mts.in.time.window.plot.s(paired.data.df[, c("Date", "Obj.CREATIVE_ENGAGEMENT")])
 # j.daily.mts.in.time.window.plot.s(paired.data.df[, c("Date", "utilization")])
  
  pause.s()
  graphics.off()
  
  
  if(Inspection<-F){
     all.metrics<-setdiff(colnames(Daily.df0), "Date") 
     nb.all.metrics<-length(all.metrics)
     for(i in 1:nb.all.metrics){
        this.metric<-all.metrics[i]
        print(this.metric)
        pause.s()
        j.daily.mts.in.time.window.plot.s1(Daily.df0[, c("Date", this.metric)], N=F, my.main.title=this.Config, Axis4.Col=NULL)
        if(i>1){
          pause.s()
          j.daily.mts.in.time.window.plot.s1(Daily.df0[, c("Date", c("utilization", this.metric))], N=F, my.main.title=this.Config, Axis2.Col=1, Axis4.Col=2)
        }
     }
  }
  
  # j.daily.mts.in.time.window.plot.s1(paired.data.df, N=T, my.main.title=this.Config, Axis4.Col=NULL)
  # pause.s()
  # graphics.off()
  
  # j.daily.mts.in.time.window.plot.s1(paired.data.df, N=F, my.main.title=this.Config, Axis2.Col=1, Axis4.Col=2)
  # sub.Daily.df<-Daily.df0[, c("Date",  "utilization", "Obj.CREATIVE_ENGAGEMENT", "Autobidding.1", "Autobidding.0")]
  # dat.df0<-Daily.df0[, -1]
  # j.daily.mts.in.time.window.plot.s(sub.Daily.df, N=T, my.main.title=this.Config)
  
  # Step 6. Modeling for the daily data at the selected segment(s) 
  
  Daily.Dat.df<-Daily.df0[, -1]
  Cor.Matrix<-cor(Daily.Dat.df)
  Cor.Row<-Cor.Matrix[1, ]
  Cor.Row.abs.Ranked<-sort(abs(Cor.Row)) [ sort(abs(Cor.Row)) > 0.4]
  Cor.Raw.Ranked<-Cor.Row[names(Cor.Row.abs.Ranked)]
  
  Response<-"utilization"
  driver.list<-setdiff(names(Cor.Raw.Ranked), Response)
  #my.formula.1 <- as.formula(paste(Response, " ~ ", paste(driver.list, collapse= "+")))
  #model.1<-lm(my.formula.1, data=Daily.Dat.df)
  # summary(model.1)
  
  
  # (Dummied) Level Effect Analysis
  #  one.day.RAW.df0<-subset(RAW.df0, Date=="20200820")
  
  # All.Continuous.Vars=c("budget", "rev_recog", "imprs")
  # All.Dummized.Vars=c("Autobidding", "Obj", "Vertical", "Tier",   "LAN",  "Country")
  
  
  causal.effect.visual.s(Data.df0=RAW.df0, ix=1)   # autobidding
  causal.effect.visual.s(Data.df0=RAW.df0, ix=2, Top.K=5)   # Obj
  causal.effect.visual.s(Data.df0=RAW.df0, ix=3, Top.K=5)   #Vertical
  causal.effect.visual.s(Data.df0=RAW.df0, ix=4)   # Tier
  causal.effect.visual.s(Data.df0=RAW.df0, ix=5)   # LAN
  causal.effect.visual.s(Data.df0=RAW.df0, ix=6, Top.K=5)   # Country
  
  causal.effects.visual.s(Data.df0=RAW.df0, Top.K=NULL) # For Autobidding+ LAN + Obj, all all levels
  causal.effects.visual.s(Data.df0=RAW.df0, Top.K=5) # For Autobidding+ LAN + Obj, all all levels
  
  
  ### # Analysis in time dimension 
  causal.effect.visual.s(Data.df0=RAW.df0, ix=1)  # absolute strength/impact of budget at each level for aoutbidding
  causal.effect.visual.s(Data.df0=RAW.df0, ix=1, Percentage=T)  # relative strength for the levels
  causal.effect.visual.s(Data.df0=RAW.df0, ix=2, Percentage=T)  # for LAN
  causal.effect.visual.s(Data.df0=RAW.df0, ix=3, Percentage=T, Top.K=3)  # for Verticals
  
  # Next: Analysis in spacial space (segments)
    
}

causal.effect.visual.s<-function(Data.df0=RAW.df0, ix=1, Top.K=NULL, Percentage=F){ 
  
  ######### 
  
  
  Raw.list<-data.filtering.s(Raw.df=Data.df0)
  Data.df0<-Raw.list$Data
  Config<-Raw.list$Config
  Selection<-Raw.list$Selection
  
  #######
  
  my.Chosen.Explantory.Continuous.Vars<- "budget"
  my.Chosen.Explantory.Dummized.Vars=c("Autobidding", "LAN", "Obj", "Vertical", "Tier",  "Country")[ix]
  my.Chosen.Response.Vars<- c("rev_recog", "imprs")[1]
  
  ######### 
  
  
  Explainatory.df<- data.daily.agg.from.dummies.s(Data.df0=Data.df0, 
                                                Chosen.Continuous.Vars=my.Chosen.Explantory.Continuous.Vars,
                                                Chosen.Dummized.Vars=my.Chosen.Explantory.Dummized.Vars)
  if(Percentage){
      tmp.df<-Explainatory.df
      Explainatory.df[, -(1:2)]<- tmp.df[, -c(1, 2)]/ tmp.df[, 2]
  }  
    
  Response.df<-daily.agg.generic.s(Data.df=Data.df0, Response.vars.to.agg= my.Chosen.Response.Vars)
  Response.Explanatory.df<-merge(Response.df, Explainatory.df)
  
  my.main<-paste("Trajectories of Revenue and Budget with its Decomposition by Levels of ", 
                 my.Chosen.Explantory.Dummized.Vars, ifelse(Percentage, " Percentage", ""), " for ", Config,  sep="")
  
  if(Percentage){
    j.daily.mts.in.time.window.plot.s(Response.Explanatory.df, Legend=F, Side=T, my.main.title=my.main,
                                    Axis2.Col = 2:3, Axis4.Col=4:dim(Response.Explanatory.df)[2], 
                                    Top.K=Top.K)
  }
  else{
   j.daily.mts.in.time.window.plot.s(Response.Explanatory.df, Legend=F, Side=T, my.main.title=my.main,
                                     Axis2.Col = 3:dim(Response.Explanatory.df)[2], Axis4.Col =2, 
                                     Top.K=Top.K)
  }
  
  # pause.s()
  if(F) j.daily.mts.in.time.window.plot.s(Response.Explanatory.df, Legend=F, Side=T, my.main.title=my.main,
                                     Axis2.Col = 2:dim(Response.Explanatory.df)[2], Normal=T, Top.K=Top.K )
  
}


causal.effects.visual.s<-function(Data.df0=RAW.df0, Top.K=NULL){ 
  
  Raw.list<-data.filtering.s(Raw.df=Data.df0)
  Data.df0<-Raw.list$Data
  Config<-Raw.list$Config
  Selection<-Raw.list$Selection
  
  #########
  
  my.Chosen.Explantory.Continuous.Vars<- "budget"
  my.Chosen.Explantory.Dummized.Vars=c("Autobidding",  "LAN", "Obj")[1]
  my.Chosen.Response.Vars<- c("rev_recog", "imprs")[1]
 
  ######### 
 
  
  Explainatory.df<- data.daily.agg.from.dummies.s(Data.df0=Data.df0, 
                                                  Chosen.Continuous.Vars=my.Chosen.Explantory.Continuous.Vars,
                                                  Chosen.Dummized.Vars=my.Chosen.Explantory.Dummized.Vars)
  
  
  Response.df<-daily.agg.generic.s(Data.df=Data.df0, Response.vars.to.agg= my.Chosen.Response.Vars)
  Response.Explanatory.df<-merge(Response.df, Explainatory.df)
  
  my.main<-paste("Trajectories of Revenue and Budget with Latter's Decompositions by Top Correlated Levels of ", 
                 paste(my.Chosen.Explantory.Dummized.Vars, collapse = " + "), " for ",  Config,  sep="")
  
  out.df<-j.daily.mts.in.time.window.plot.s(Response.Explanatory.df, Legend=F, Side=T, my.main.title=my.main,
                                    Axis2.Col = 3:dim(Response.Explanatory.df)[2], Axis4.Col =2, 
                                    Top.K=Top.K)
  
  pause.s()
  j.daily.mts.in.time.window.plot.s(Response.Explanatory.df, Legend=F, Side=T, my.main.title=my.main,
                                    Axis2.Col = 2:dim(Response.Explanatory.df)[2], Normal=T, Top.K=Top.K )
  
 
  out.df
  
  
}

response.modeling.s<-function(Data.df0=RAW.df0, Top.K=NULL){
  identified.data.df<-causal.effects.visual.s(Data.df0=Data.df0, Top.K=5)
  Response<-"rev_recog"
  driver.list<-  setdiff(names(identified.data.df), Response)
  my.formula.1 <- as.formula(paste(Response, " ~ ", paste(driver.list, collapse= "+")))
  
  model.1<-lm( my.formula.1, data=identified.data.df)
  summary(model.1)
  
}

auto.magnitude.determination.s<-function(x=rnorm(100, 1.2, 100)){
   # Input: x is a numeric object such as a vector, a matrix, a time series or multiple of them.
   # Output: the magnitude of the object: 10^2, 10^3, 10^4, 10^5, 10^6 
   
    magnitude<-0
    x<-median(abs(x), na.rm=T)
    Stop<-F
    i<-0
    while(!Stop){
      comp<-10^i
      if(x/comp < 10){
         magnitude<-i
         Stop<-T
      }
      else{
        i<-i+1
      }
   }
    magnitude
}

daily.agg.generic.s<-function(Data.df, Response.vars.to.agg){
  
  my.formula.txt<-paste("cbind(", paste(Response.vars.to.agg, collapse=","), ") ~ Date", sep="") 
  my.formula<-as.formula(my.formula.txt)
  
  daily.agg.df<-aggregate(my.formula, data=Data.df, FUN=sum)
  
  daily.agg.df
  
  
}

lasso.regression.generic.s<-function(data.df, testing.periods=2){
  
  #  library(glmnet)
  #  library(plyr)
  #  library(readr)
  #  library(dplyr)  #to use glimpse function 
  #  library(caret)  #to use preProcess
  #  library(ggplot2)
  #  library(repr)
  
  #graphics.off()
  monthly.input.list<- data.input.202008.s(Last.Complete.Month="202007")
  
  
  MDat.df0<-monthly.input.list$Monthly.df
  Monthly.Rev.ts<-Monthly.df.to.ts.s(MDat.df0[, c("Month", "Gross.Rev")])
  
  MDat.df<-MDat.df0[, -1]
  
  browser()
  
  #glimpse(MDat.df)
  
  X.df <- model.matrix(Gross.Rev~. , data=MDat.df)[,-1]
  y.var <- MDat.df$Gross.Rev
  lambda.candidates <- 10^seq(2, -2, by = -.1)
  
  # Splitting the data into test and train
  set.seed(86)
  # training.index = sample(1:nrow(X.df), nrow(X.df)/2)
  
  training.index = 1:(nrow( MDat.df)-testing.months)
  
  train.df<-MDat.df[training.index , ]
  test.df<-MDat.df[-training.index , ]
  
  # Scaling the numeric features
  
  all.expl.cols = setdiff(colnames(MDat.df), "Gross.Rev")
  
  pre_proc_val <- preProcess(train.df[,  all.expl.cols], method = c("center", "scale"))
  
  train.df[, all.expl.cols] = predict(pre_proc_val, train.df[, all.expl.cols])
  test.df[, all.expl.cols] = predict(pre_proc_val, test.df[, all.expl.cols])
  
  # summary(train.df)
  
  # lm
  
  lr.model<-lm(Gross.Rev~., data=train.df)
  
  summary(lr.model)
  
  
  # Step 2 - predicting and evaluating the model on train data
  
  predictions.for.train<-predict(lr.model, newdata = train.df)
  Actual.Fitted<-cbind(Actual=train.df[, "Gross.Rev"], Fitted=predictions.for.train)
  
  eval.metrics.s(lr.model, train.df, predictions.for.train, target = 'Gross.Rev')
  
  # Step 3 - predicting and evaluating the model on test data
  predictions.for.test = predict(lr.model, newdata = test.df)
  Actual.Fcasted<-cbind(Actual=test.df[, "Gross.Rev"], Fcasted=predictions.for.test)
  
  eval.metrics.s(lr.model, test.df, predictions.for.test, target = 'Gross.Rev')
  
  
  # Use traing sample data to find the best lambda value and the resulted best bestlasso model.
  
  cv_output <- cv.glmnet(X.df[training.index,], y.var[training.index],   # cv.glmnet does k-fold cross-validation for glmnet which fits a GLM with lasso.
                         alpha = 1, lambda = lambda.candidates)
  best.lambda <- cv_output$lambda.min
  best.lasso <- glmnet(X.df[training.index,], y.var[training.index], alpha = 1, lambda = best.lambda)
  coef(best.lasso)
  
  testing.index<-(1:nrow(X.df))[-training.index]
  pred <- predict(best.lasso, s = best.lambda, newx = X.df[testing.index, ])
  
  # eval_results(y_train, predictions_train, train)
  
  testing.index<-(1:nrow(X.df))[-training.index]
  training.start.month<-tsp.back.s(tsp(Monthly.Rev.ts))$start.month.name
  my.testing.start<-kmonths.apart.month.s(ref.month=training.start.month, length(training.index))
  pred.ts<-ts(pred, start=as.numeric(c(substring(my.testing.start, 1, 4), substring(my.testing.start, 5, 6))),  freq=12)
  
  # Checking the first six obs
  final.df <- data.frame(Actual=y.var[testing.index], Predicted= as.vector(pred))
  AF.ts<-cbind(Actual=Monthly.Rev.ts, Pred=pred.ts)
  
  dev.new(width=16, height=10)
  j.tsplot.s(AF.ts, my.title="Monthly Actual vs. Prediction", my.ylab="Revenue")
  
  head(final.df)
  
  MAPE<-error.s(final.df)
  ##########
  if(F){
    Response<-"Gross.Rev"
    driver.list<-  c("eCPC",  "Ad.Imprs")
    my.formula.1 <- as.formula(paste(Response, " ~ ", paste(driver.list, collapse= "+")))
    my.formula.1 <- Gross.Rev ~ eCPC + Ad.Imprs -1
    
    model.lm.1<-lm(my.formula.1, data=MDat.df)
    summary(model.lm.1)
  }
  
  list(lr.model=lr.model, Lasso.model=best.lasso, Coeff.Lasso=round(best.lasso$beta, d=4), MAPE=MAPE)
}


j.daily.af.in.time.window.plot.s<-function(Data.df, my.cex=2, pch=1, TYPE="o", extra=3, my.cex.lab=0.5, my.main.title="", my.ylab="Actual vs Fcast", additional.text="") 
{
  
  if(missing(Data.df)) {
    my.dates<-seq.date(20200701, 20200828)
    Data.df<-data.frame(Date=my.dates, Actual=rnorm(length(my.dates)), Forecast=rnorm(length(my.dates))+0.3)
    # Data.df has 4 cols: Date, Actual, and Forecast. That is, DAFE without E.
  }
  browser()
  AF.df<-Data.df[, 2:3]
  
  my.ylim<-c(min(AF.df, na.rm=T), max(AF.df, na.rm=T)*1.07)
  delta<-my.ylim[2]-my.ylim[1]
  nb.days<-dim(AF.df)[1]
  
  my.cex.lab<-0.8
  all.at<-seq(from=1, by=1, to=nb.days)
  
  all.dates<-Data.df$Date
  date.string<-paste(substring(all.dates, 1, 4), "-", 
                     substring(all.dates, 5, 6), "-",
                     substring(all.dates, 7, 8), sep="")
  
  all.DOW<-day.of.week.s(all.dates)
  all.M.at<- all.at[is.element(all.DOW, c("Mon"))]
  
  all.M.labels.1<-rep("", length(all.M.at))
  all.M.labels.2<-paste(date.string[all.M.at], "-", all.DOW[all.M.at], " ", sep="")
  all.labels <-paste(date.string, "-",  all.DOW, sep="")
  
  dat.extra<-data.frame(Y1=rep(NA, extra), Y2=rep(NA, extra))
  colnames(dat.extra)<-colnames(Data.df)[-1]
  dat.extended<-rbind(AF.df, dat.extra)
  colnames(dat.extended)<-colnames(Data.df)[-1]
  
  ts.plot(dat.extended, gpars=list(type="n", axes=F, col=1, xlab="",
                                   ylab="", cex.lab=my.cex.lab,
                                   ylim=my.ylim))
  
  par(new=F)
  
  ts.plot(dat.extended, gpars=list(type=TYPE, axes=F, col=1:2, xlab="", lty=1:2,
                                   ylab=my.ylab,  ylim=my.ylim, main=my.main.title))
  box()
  grid(nx=NA, ny=NULL)
  
  
  #######
  
  Error.at.all<-round(100*(AF.df[, 2]-AF.df[, 1])/AF.df[, 1], d=2)
  red.zone<-all.at[abs(Error.at.all)>8]
  orange.zone<-all.at[abs(Error.at.all)<=8 & abs(Error.at.all) > 4]
  blue.zone<-all.at[ abs(Error.at.all) <=4]
  
  axis(side=1, at=all.M.at, labels =all.M.labels.1, cex.axis=my.cex.lab, las=2, tck=0.01)
  all.M.except.red.at<-setdiff(all.at[(all.DOW =="Mon")], red.zone)
  
  all.strings<-paste(date.string, "-",  all.DOW, " ", sep="")
  all.M.except.red.txt<-all.strings[all.M.except.red.at]
  
  
  text(x = all.M.except.red.at, y = par("usr")[3] - 0.025*delta,
       labels =  all.M.except.red.txt, xpd = NA, srt = 90, cex = 0.50, adj=0.85, col="blue")
  
  all.except.M.and.red.at<-setdiff(all.at[all.DOW !="Mon"], red.zone)
  all.except.M.and.red.txt<-all.strings[all.except.M.and.red.at] 
  
  text(x = all.except.M.and.red.at, y = par("usr")[3] - 0.025*delta,
       labels = all.except.M.and.red.txt, xpd = NA, srt = 90, cex = 0.50, adj=0.85, col="black")
  
  Red<- (length(red.zone)> 0) 
  Blue<-(length(blue.zone)> 0) 
  Orange<-(length(orange.zone)> 0) 
  
  text(x = -1.5, y = par("usr")[4] + 0.025*delta, labels = "Error% =>", xpd = NA, srt = 0, cex = 0.65, adj=0.5, col="black")
  
  if(Red) {
    text(x = all.at[red.zone], y = par("usr")[3] - 0.025*delta,
         labels =  all.labels[red.zone], xpd = NA, srt = 90, cex = 0.50, adj=0.85, col="red")
    text(x = all.at[red.zone], y = par("usr")[4] + 0.025*delta,
         labels = Error.at.all[red.zone], xpd = NA, srt = 90, cex = 0.45, adj=0.5, col="red")
  }
  if(Orange)
    text(x = all.at[orange.zone], y = par("usr")[4] + 0.025*delta,
         labels = Error.at.all[orange.zone], xpd = NA, srt = 90, cex = 0.45, adj=0.5, col="orange")
  if(Blue)
    text(x = all.at[blue.zone], y = par("usr")[4] + 0.025*delta,
         labels = Error.at.all[blue.zone], xpd = NA, srt = 90, cex = 0.45, adj=0.5, col="blue")
  
  axis(side=2, cex.axis=my.cex.lab)
  abline(v=all.at, lty=1, col="lightgray")
  abline(v=all.M.at, lty=1, col="blue")
  
  my.legend.txt<-colnames(Data.df)[-1]
  my.legend.txt[2]<-paste(my.legend.txt[2], additional.text, sep=" ")
  
  legend(mean(all.at), my.ylim[2], legend=my.legend.txt, col=1:2, xjust=0.5, horiz=T, 
         lty=1:2, cex=0.5, bg="white")
}


normalize.s<-function(x){
    (x-min(x, na.rm=T))/ (max(x, na.rm=T)-min(x, na.rm=T))
}

j.as.data.frame.s<-function(dat.df, column.names="Column"){
  
  if(!is.data.frame(dat.df)) {
      dat.df2<-as.data.frame(dat.df)
      colnames(dat.df2)<-column.names
  }
  else{
    dat.df2<-dat.df
  }
  dat.df2
}

top.correlation.s<-function(Dat.df, Top.K=NULL){
  # Input: (1) the response var in the 1st column, and explanatory vars in other columns.
  #        (2) The number specification for the top most correlated explanatory variables. If K=NULL, then list all. 
  #            Otherwise, K is trankated by the max expaniantory vars (= dim(Dat.df)[2]-1. )
  # Output: (1) sorted correlations between the response and all the explanatory variables when K =NULL
  #         (2) names of the explanatory vars that are most correlated to the response var when K is specified. 
  # 
  
  Correlation<- cor(Dat.df)[1, ][-1] 
  resp.var<-colnames(Dat.df)[1]
  #browser()
  Out.K<-min(Top.K, dim(Dat.df)[2]-1)  # applicable to Top.K =NULL
  Out.K.corr.abs<- sort(abs(Correlation), decreasing = T)[1:Out.K]
  Out.K.vars<-names(Out.K.corr.abs)
  Out.K.corr<-Correlation[Out.K.vars]
  Out.K.Dat.df<-Dat.df[, c(resp.var, Out.K.vars)]
  list(Out.K.Dat.df=Out.K.Dat.df, Out.K.corr=Out.K.corr, Out.K.vars=Out.K.vars, Top.K=Top.K)
  
}


#par(mar=c(5.1, 4.1, 4.1, 4.1))
j.daily.mts.in.time.window.plot.s<-function(Data.df, my.cex=2, pch=1, TYPE="o", extra=12, my.cex.lab=0.5, my.main.title="", 
                                             my.ylab="Daily Values for ", additional.text="", Normalization=F, 
                                             Axis2.Col=2:dim(Data.df)[2],  # the numbers are in reference to the Data.df
                                             Axis4.Col=NULL, 
                                             Side.Notes=T, Legend=F, Top.K=5) 
{
  # 
  # multiple daily time series over a long period. 
  par(mar=c(5.1, 4.1, 4.1, 3.1)) 
 
 
  if(missing(Data.df)) {
    my.dates<-seq.date(20190701, 20201028)
    Data.df<-data.frame(Date=my.dates, X=rnorm(length(my.dates), sd=0.1), Y=rnorm(length(my.dates), sd=0.1)+ 1, Z= rnorm(length(my.dates), sd=0.1)+ 2)
  }
  
  if(dim(Data.df)[2]==2){
     ncol.dat.df<-1
     Dat.df0<-Dat.df<-j.as.data.frame.s(Data.df[, -1], colnames(Data.df)[-1])
  }
  else{
    Dat.df0<-Dat.df<-Data.df[, -1]  # Dat.df is without the date column, while Data.df contains the date column.
  }
  
  all.col.names.in.Data<-colnames(Data.df)
  all.col.nbs.in.Data<-1:length(all.col.names.in.Data)
  
  names(all.col.names.in.Data)<-all.col.nbs.in.Data
  
  Axis2.Col.names<-all.col.names.in.Data[Axis2.Col]
  Axis4.Col.names<-all.col.names.in.Data[Axis4.Col]
  
  
  Top.Corr.list<-top.correlation.s(Dat.df=Dat.df0, Top.K=Top.K)
  Dat.df0<-Top.Corr.list$Out.K.Dat.df
  Top.col.names<-Top.Corr.list$Out.K.vars

  All.col.names.in.Top.df<-colnames(Dat.df0)
  Axis2.Col.names<-intersect(Axis2.Col.names, All.col.names.in.Top.df)
  Axis4.Col.names<-intersect(Axis4.Col.names, All.col.names.in.Top.df)
  
  correlation.txt<-c("", paste("(r=", round(100*Top.Corr.list$Out.K.corr, d=2), "%)", sep=""))
  names(correlation.txt)<-colnames(Dat.df0)
  notes.txt<-paste(colnames(Dat.df0),  correlation.txt, sep=" ")
  names(notes.txt)<-colnames(Dat.df0)

  if(dim(Dat.df0)[2]==1){
    my.main.title<-paste(colnames(Dat.df), "for",  my.main.title)
  }
  else if (dim(Dat.df0)[2]==2){
    my.main.title<-paste(colnames(Dat.df)[1], "vs.", colnames(Dat.df)[2], "for",  my.main.title)
  }
  
  # Dat.df<-as.data.frame(Dat.df)
  # names(Dat.df)<-names(Data.df)[-1]
   
  nb.days<-dim(Dat.df)[1]
  
  my.cex.lab<-0.6
  all.at<-seq(from=1, by=1, to=nb.days)
  
  all.dates<-Data.df$Date
  names(all.at)<-all.dates
  
  date.string<-paste(substring(all.dates, 1, 4), "-", 
                     substring(all.dates, 5, 6), "-",
                     substring(all.dates, 7, 8), sep="")
  
  all.DOW<-day.of.week.s(all.dates)
  all.M.at<- all.at[is.element(all.DOW, c("Sat"))]
  all.labels <-paste(date.string, " (",  all.DOW, ")", sep="")
  
  if(Normalization){
      for(i in 1:dim(Dat.df0)[2]){
         Dat.df0[, i]<- normalize.s(Dat.df0[, i])
      }
      my.ylab<-paste(my.ylab, "(Normalized)", sep="")
  }
  
  AX<-numeric(0)
  
  if(AX2<-!is.null(Axis2.Col)){
      AX<-c(AX, 2)
      
      Dat.df2<-j.as.data.frame.s(Dat.df0[, Axis2.Col.names], Axis2.Col.names)
  }
  if(AX4<-!is.null(Axis4.Col)){
      AX<-c(AX, 4)
      Dat.df4<-j.as.data.frame.s(Dat.df0[, Axis4.Col.names], Axis4.Col.names) 
  }
  
  for(ax in AX){
      if(ax==2) {
         Dat.df<-Dat.df2
         Axis.Col<-Axis2.Col
         Axis.Key<-paste(colnames(Data.df)[ Axis.Col], collapse = " + ")
         }
      else {
         Dat.df<-Dat.df4
         Axis.Col<-Axis4.Col
        # browser()
         all.vars<-colnames(Data.df)[ Axis.Col]
         Axis.Key<-paste(all.vars[1], " + Other Level(s)", sep="")
         par(new=T)
      }
    
      my.magnitude<-auto.magnitude.determination.s(as.matrix(Dat.df))
      Dat.df<-Dat.df/10^my.magnitude
      
      y.lim.added<-(max(Dat.df, na.rm=T)-min(Dat.df, na.rm=T))/(6*2)
      last.day.x.value<-dim(Dat.df)[1]
      last.day.y.values<-as.numeric(Dat.df[last.day.x.value, ])
      
      my.ylim<-c(min(Dat.df, na.rm=T), max(Dat.df, na.rm=T)+y.lim.added)
      delta<-my.ylim[2]-my.ylim[1]
  
      dat.extra.matrix<-matrix(NA, ncol= dim(Dat.df)[2], nrow=extra)
      dat.extra.df<-as.data.frame(dat.extra.matrix)
      colnames(dat.extra.df)<-colnames(Dat.df)
      dat.extended<-rbind(Dat.df, dat.extra.df)
      colnames(dat.extended)<-colnames(Dat.df)
  
      my.ylab.added<-paste(my.ylab, " ", Axis.Key, ifelse(Normalization, "", 
                                                          paste(" (in 10^", my.magnitude, ")", sep="")),  sep="")
      
      if(ax==2) {
        
        ts.plot(dat.extended, gpars=list(type="n", axes=F, col=1, xlab="",
                                         ylab="", cex.lab=my.cex.lab,
                                         ylim=my.ylim, cex=0.8))
        ts.plot(dat.extended, gpars=list(type=TYPE, axes=F, col=Axis.Col, xlab="", lty=1,
                                         ylab="",  ylim=my.ylim, main=my.main.title, cex=0.8))
        mtext(my.ylab.added, side=2, line = 2, col="black", cex=0.8)
        #par(new=F)
      }
      else{
        par(new=T)
        ts.plot(dat.extended, gpars=list(type=TYPE, axes=F, col=Axis.Col, xlab="", lty=1,
                                         ylab="",  ylim=my.ylim, main=my.main.title, cex=0.8))
        mtext(my.ylab.added, side=4, line = 2, col="black", cex=0.8)
      }
  
      if(length(AX)==1 | (length(AX)==2 & ax==2))  {
        box()
      }
      grid(nx=NA, ny=NULL)
      
      axis(side=ax, cex.axis=my.cex.lab)
      last.day.x.values<-rep(last.day.x.value, length(last.day.y.values))
      if(F) arrows(x0=last.day.x.values, y0=last.day.y.values, 
             x1=last.day.x.values+3, y1=last.day.y.values, col=Axis.Col, length = 0.15)
      
      if(Side.Notes) {
         notes.labels<-notes.txt[colnames(dat.extended)]
         text(x=last.day.x.values+1, y= last.day.y.values, labels=notes.labels, col=Axis.Col, cex=0.5, adj=0 )
      }
  }
  
  abline(v=all.at, lty=1, col="lightgray")
  abline(v=all.M.at, lty=3, col="blue")

  text(x = all.at, y = par("usr")[3] - 0.025*delta,
       labels =  all.labels, xpd = NA, srt = 65, cex = 0.50, adj=0.85, col="black")
  axis(side=1,  at= all.at, 
       labels=rep("", length(all.at)), # col="black",
       tck=-0.01)
  
  all.months<-sort(unique(substring(all.dates, 1, 6)))
 
  for(i in 1:length(all.months)){
    this.month<-all.months[i]
    nb.days.in.this.month<-daysInMonth(this.month)
    dates.for.this.month.ends<-paste(this.month, c("01", nb.days.in.this.month), sep="")
    effective.dates.for.this.month.ends<-intersect(all.dates,  dates.for.this.month.ends)
    effective.this.month.ends.at<- all.at[effective.dates.for.this.month.ends]
    axis(side=3,  at= effective.this.month.ends.at, 
         labels=rep("", length(effective.this.month.ends.at)), col="blue",
         tck=-0.01)
    abline(v=effective.this.month.ends.at[1], lty=2, col="blue")
    if(length(effective.this.month.ends.at) >1)
       abline(v=effective.this.month.ends.at[2], lty=1, col="blue")
    
    this.month.hyphened<-paste(substring(this.month, 1, 4), "-", substring(this.month, 5, 6), sep="")
    if(length(effective.this.month.ends.at)==2){
      my.label<-paste("<-", this.month.hyphened,  "->", sep="")
      my.adj<-0.5
    }
    else{
       if(substring(effective.dates.for.this.month.ends, 7, 8)=="01"){
         my.label<-paste(this.month.hyphened, "->", sep="")
         my.adj<-0
       }
       else{
         my.label<-paste(this.month.hyphened, "<-", sep="")
         my.adj<-1
       }
    }
    text(x=mean(effective.this.month.ends.at), y = par("usr")[4] + 0.02*delta,
              labels =  my.label, 
              xpd = NA, srt = 0, cex = 0.50, adj=my.adj, col="blue") 
       
  }
  
  if(dim(Dat.df0)[2]>1 & Legend){
      legend(mean(all.at), my.ylim[2], legend=notes.txt, col=seq(1, dim(Dat.df0)[2]), xjust=0.5, horiz=T, 
         lty=1, cex=0.5, bg="white")
  }
  Dat.df0
 
}


