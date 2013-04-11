#### comparaison entre les sorties #### 


nom = "C:/Myliam2/output OF/"
en1 = read.csv(paste0(nom,"en1.csv"),stringsAsFactors=FALSE)
ind = read.csv(paste0(nom,"ind.csv"),stringsAsFactors=FALSE)
fam = read.csv(paste0(nom,"fam.csv"),stringsAsFactors=FALSE)
foy = read.csv(paste0(nom,"foy.csv"),stringsAsFactors=FALSE)
men = read.csv(paste0(nom,"men.csv"),stringsAsFactors=FALSE)

list_diff = c()
list_diff.ent = c()
list_same = c()
list_same.ent = c()
for (nam in names(en1)) {
  for (ent in c("ind","fam","foy","men")){
    if (nam %in% names(get(ent))){
      print(paste(nam,ent))
      if (all(en1[nam] == get(ent)[nam])) {
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
## -> les differences ne sont que sur les entite non ind