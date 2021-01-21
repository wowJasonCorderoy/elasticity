library(tidyverse)
library(arules)

## inputs
set.seed(666)
n_Articles <- 100
n_baskets <- 1000

## generate probability of article being first purchase in store
simArticleProb <- function(n){
  rep(1, n)/n
}
p_Article <- simArticleProb(n=n_Articles)

## Generate article transition matrix:
t_Articles <- matrix( runif(length(t_Articles), 0.01, 0.5), n_Articles, n_Articles)

# force zero diagonal
diag(t_Articles) <- 0

# Percentages: Rowwise sums to 1
axisToPerc <- function(dat, axis){
  (dat/apply(dat, axis, sum))
}
t_Articles <- axisToPerc(t_Articles, 1)

df_t_Articles <- data.frame(lhs_numeric = 1:n_Articles, rhs_numeric = 1:n_Articles, value = c(t_Articles))

checkPerc <- function(dat, axis, digits=2){
  all( round(apply(dat, axis, sum),digits)==1 )
}
checkPerc(t_Articles, axis=1)

## how many articles per basket
simArticlesPerBasket <- function(n, min, max){
  round( runif(n_baskets, min, max), 0)
}
articlesPerBasket <- simArticlesPerBasket(n_baskets, 1, min(n_Articles/2, 100))

## First article per basket
simFirstArticlePerBasket <- function(n, p){
  replicate(n_baskets, 
            sample(1:length(p), 1, prob = p))
}

basketFirstArticle <- simFirstArticlePerBasket(n=n_baskets, p=p_Article)

ll <- list()
for(i in 1:n_baskets){
  i_articlesPerBasket = articlesPerBasket[i]
  i_basketFirstArticle <- basketFirstArticle[i]
  
  elementName <- paste0("Basket ", i)
  
  ll[[elementName]] <- i_basketFirstArticle
  if(i_articlesPerBasket == 1){next}
  
  while(length(ll[[i]])<i_articlesPerBasket){
    currentArticle <- tail(ll[[elementName]],1)
    
    alreadyInBasket <- unique(ll[[elementName]])
    adjusted_t_Articles <- t_Articles
    adjusted_t_Articles[,alreadyInBasket] <- 0
    adjusted_t_Articles <- axisToPerc(adjusted_t_Articles, 1)
    
    newArticleSampled <- sample(1:n_Articles, 1, prob=adjusted_t_Articles[currentArticle,])
    ll[[elementName]] <- c(ll[[elementName]], newArticleSampled)
  }
}
# check only distinct articles in each basket:
unlist( lapply(ll, function(x){length(x)==length(unique(x))}) )

# convert basket list to matrix
mm <- matrix(FALSE, n_baskets, n_Articles)
for(i in 1:n_baskets){
  mm[i, ll[[i]]] <- TRUE
}

## run arules
rules <- apriori(mm, parameter = list(supp = 0, minlen=1, maxlen=2, conf = 0.001, target = "rules"))
summary(rules)

#inspect(head(rules, by = "lift"))

rules2df <- function(rules){
  df_rules <- data.frame( inspect(rules) )
  df_rules$lhs_numeric <- gsub("[{}]","",df_rules$lhs) %>% as.numeric()
  df_rules$rhs_numeric <- gsub("[{}]","",df_rules$rhs) %>% as.numeric()
  return(df_rules)
}

df_rules <- rules2df(rules)

#df_rules %>% left_join(df_t_Articles)


