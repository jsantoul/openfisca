#### comparaison entre les sorties #### 


nom = "C:/openfisca/output/"
en1 = read.csv(paste0(nom,"en1.csv"),stringsAsFactors=FALSE)
ind = read.csv(paste0(nom,"ind.csv"),stringsAsFactors=FALSE)
fam = read.csv(paste0(nom,"fam.csv"),stringsAsFactors=FALSE)
foy = read.csv(paste0(nom,"foy.csv"),stringsAsFactors=FALSE)
men = read.csv(paste0(nom,"men.csv"),stringsAsFactors=FALSE)


#on tranforme les tables pour faire partir les true et false
for (ent in c("en1","ind","fam","foy","men")){
  temp = apply(get(ent), 2, as.character)
  temp[temp=="False"] = "0"
  temp[temp=="True"] =  "1"
  assign(ent, apply(temp, 2, as.numeric) )
}

list_diff = c()
list_diff.ent = c()
list_same = c()
list_same.ent = c()
for (nam in colnames(en1)) {
  for (ent in c("ind","fam","foy","men")){
    if (nam %in% colnames(get(ent))){
      print(paste(nam,ent))
      if (all(en1[,nam] == get(ent)[,nam])) {
        list_same = c(list_same,nam)
        list_same.ent = c(list_same.ent,ent)        
      }
      else {
        list_diff = c(list_diff,nam)
        list_diff.ent = c(list_diff.ent,ent)        
      }
    }
  }
}
## variables individuel
list.ind = list_diff[which(list_diff.ent=='ind')]
# to check: variable dependant de variable collective
en1[,list.ind] - ind[,list.ind]

## variables foy
list.foy = list_diff[which(list_diff.ent=='foy')]
# to check: variable dependant de variable collective
voir1 = en1[,list.foy]
voir3 = foy[,list.foy]
voirDiff = voir3 - voir1
