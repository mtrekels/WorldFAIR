library(dplyr)
library(stringr)
library(plyr)
library(tidyr)
library(shadowtext)
library(hrbrthemes)

source('functions.R')

data.list <- list(dryad=NULL, dataone=NULL,zenodo=NULL,figshare=NULL)
repositories <- c('dryad', 'dataone', 'zenodo', 'figshare')

# Search repositories for each term
term = "plant pollinator agriculture"
print(term)
for (repo in repositories) {
  print(paste("Repository", repo))
  file <- paste0('datasets/', repo,'-',term, '.rds')
  if(!file.exists(file)) {
    print("RDS file not found. Retrieving datasets from API...")
    data <- do.call(paste0('get_', repo, '_datasets'), args = list(term=term))
    print(paste("Saving to RDS file:", file))
    saveRDS(data, file=file)
    data.list[[repo]] <- bind_rows(data.list[[repo]], data)
  } else {
    print("RDS file found. Loading data...")
    aux <- readRDS(file)
    data.list[[repo]] <- data.list[[repo]] %>%
      bind_rows(aux)
  }
}

# Fix dataone date format
data.list[["dataone"]]$pubDate <- format.Date(data.list[["dataone"]]$pubDate, "%Y-%m-%d")

# Bind all datasets into one data.frame
# TODO: investigate data for more metadata (licence, persistent identifiers (multiple), standards, provenance, preservation policy)
data.list <- lapply(data.list, mutate, alternateId=as.character(alternateId))
data.all <- dplyr::bind_rows(data.list, .id="source")

write.table(data.all, file='../results/datasets.tsv', sep='\t', row.names=F)

data.all <- read.csv('datasets/datasets.tsv', sep = "\t")
data.all <- data.all %>% as_tibble()
data.all

# Remove duplicate datasets
data.uniq <- data.all %>%
  arrange(id,desc(citations),desc(downloads),desc(views)) %>%
  distinct(source,id, .keep_all = T)
dim(data.uniq)

data.uniq <- data.uniq %>%
  arrange(title,pubYear,desc(citations),desc(downloads), desc(views)) %>%
  dplyr::distinct(title,pubYear, .keep_all = T)
dim(data.uniq)

data.uniq %>%
  dplyr::count(source)

# Convert dates to year and merge inter-specific and interspecific
data.uniq <- data.uniq %>%
  dplyr::mutate(pubYear=as.integer(format(as.Date(pubDate),"%Y"))) %>%
  mutate(term=stringr::str_remove(term,"-"))

# Datasets by publication year
plt <- data.uniq %>%
  dplyr::filter(!is.na(pubYear)) %>%
  dplyr::distinct(title,pubYear, .keep_all = T) %>%
  dplyr::count(pubYear) %>%
  ggplot(aes(x=pubYear, y=cumsum(n))) +
  geom_point(size=3, shape=16, color='#076fa2') +
  geom_line(linewidth=1, color='#076fa2') +
  coord_cartesian(clip = "off") +
  labs(title = "Cumulative number of datasets per year", subtitle = stringr::str_wrap('Accumlated number of datasets published from 2003 to 2023 in Dryad, DataONE, Figshare and Zenodo', 60)) +
  xlab("Year") +
  ylab('Number of datasets') +
  scale_x_continuous(breaks = seq(2003, 2023, 4)) +
  scale_y_continuous() +
  theme(
    plot.title = element_text(size=12, face='bold'),
    plot.subtitle = element_text(size=10),
    axis.title.y = element_text(size=12, face='bold'),
    axis.title.x = element_text(size=12, face='bold', margin = margin(10,0,0,0,'pt')),
    axis.text.x = element_text(size=10, angle = 30, hjust = 0.5, vjust = 0.5),
    axis.text.y = element_text(size=10),
  )
plt
ggsave('../results/cum-datasets-year.png', plot = plt, dpi = 300)


# Remove dryad datasets from DataONE and Zenodo
data.filtered <- data.uniq %>%
  filter((!stringr::str_detect(id, "dryad") & (source %in% c("zenodo","dataone"))) | source %in% c('dryad', 'figshare'))
dim(data.filtered)

# Top citations/downloaded datasets
data.uniq %>%
  arrange(desc(citations)) %>%
  select(id,alternateId,title,citations,downloads, views)

# Citations, downloads and views
# Remove datasets with publications DOI instead of dataset DOI (e.g. PLOSOne DOI)
journal.suffix <- data.uniq %>%
  mutate(id=stringr::str_remove(id, "doi:")) %>%
  filter(stringr::str_detect(id, "\\.s\\d+$") & !stringr::str_detect(id,"dryad")) %>%
  mutate(suffix=stringr::str_replace_all(id, "^.*/", "")) %>%
  mutate(suffix=stringr::str_split_i(suffix, "\\.", 1)) %>%
  distinct(suffix)
journal.suffix$suffix

metrics.clean <- data.uniq %>%
  mutate(id=stringr::str_remove(id, "doi:")) %>%
  filter(!grepl(paste(journal.suffix$suffix, collapse = '|'), id)) %>%
  select(id, source, citations, downloads, views)
dim(metrics.clean)

metrics.clean %>%
  summarise(tc=sum(citations), tcm=mean(citations), sdcm= sd(citations) )

# Metrics by source
metrics.all <- metrics.clean %>%
  pivot_longer(c('downloads', 'citations', 'views'), names_to='metric', values_to = 'value')

metrics.all %>%
  group_by(source, metric) %>%
  dplyr::summarise(value=sum(value))

data.uniq  %>%
  group_by(source) %>%
  dplyr::summarise(citations=sum(citations))

metrics <- metrics.all %>%
  group_by(metric) %>%
  dplyr::summarise(value=sum(value))
metrics

plt <- metrics %>%
  ggplot() +
  geom_col(aes(value,metric), fill='#076fa2', width=0.8) +
  scale_x_continuous(
    #limits=c(0, 86700), #TODO: fix limits
    #breaks = seq(0,max(metrics$value),1000), #TODO: fix breaks
    expand = c(0, 0),
    position = 'top',
    labels = scales::label_number_si()
  ) +
  scale_y_discrete(
    expand = expansion(add=c(0,0.5))
  ) +
  theme(
    panel.background = element_rect(fill="white"),
    panel.grid.major.x = element_line(color='#A8BAC4', linewidth = 0.3),
    axis.ticks.length = unit(0, 'mm'),
    axis.title = element_blank(),
    axis.line.y.left = element_line(color='black'),
    axis.text.y = element_blank(),
    axis.text.x = element_text(family = 'Econ Sans Cnd', size = 10)
  ) +
  geom_shadowtext(
    data =  subset(metrics, value < 3550),
    aes(value+20, y = metric, label = metric),
    hjust = 0,
    nudge_x = 0.3,
    colour = '#076fa2',
    bg.colour = 'white',
    bg.r = 0.2,
    family = 'Econ Sans Cnd',
    size = 5
  ) + geom_text(
    data = subset(metrics, value >= 3550),
    aes(30,y=metric,label=metric),
    hjust = 0,
    nudge_x = 0.3,
    colour = "white",
    family = 'Econ Sans Cnd',
    size = 5
  ) +
  labs(
    title="Dataset Metrics",
    subtitle = 'Total number of downloads, views and citations'
  ) +
  theme(
    plot.title = element_text(
      family = 'Econ Sans Cnd',
      face = 'bold',
      size = 16
    ),
    plot.subtitle = element_text(
      family = 'Econ Sans Cnd',
      size=14
    )
  ) +
  theme(plot.margin = margin(0.01, 0.05, 0.02, 0.01, "npc"))
plt


# Licenses
licenses <- data.uniq

licenses[is.na(licenses$license),]$license <- 'None'
licenses[licenses$license == "https://creativecommons.org/publicdomain/zero/1.0/",]$license <- 'CC0 1.0'
licenses[licenses$license == "https://creativecommons.org/licenses/by/4.0/",]$license <- 'CC-BY 4.0'
licenses[licenses$license == "https://creativecommons.org/licenses/by-nc/4.0/",]$license <- 'CC-BY-NC 4.0'
licenses[licenses$license == "https://creativecommons.org/licenses/by-nc-sa/4.0/",]$license <- 'CC-BY-NC-SA 4.0'
licenses[licenses$license == "https://creativecommons.org/licenses/by-nc/3.0/",]$license < 'CC-BY-NC 3.0'
licenses[licenses$license == "https://creativecommons.org/licenses/by-sa/4.0/",]$license <- 'CC-BY-SA 4.0'
licenses[licenses$license == "https://creativecommons.org/licenses/by/3.0/us/",]$license <- 'CC-BY 3.0 US'
licenses[licenses$license == "https://www.gnu.org/copyleft/gpl.html",]$license <- 'GPL'
licenses[licenses$license == "https://opensource.org/licenses/MIT",]$license <- 'MIT'
licenses[licenses$license == "https://www.gnu.org/licenses/gpl-3.0.html",]$license <- 'GPL-3.0'
licenses[licenses$license == "https://creativecommons.org/licenses/by-nc-nd/4.0/",]$license <- 'CC-BY-ND-4.0'
licenses[licenses$license == "http://rightsstatements.org/vocab/InC/1.0/",]$license <- 'In Copyright'
licenses[licenses$license == "https://opendatacommons.org/licenses/by/summary/index.html",]$license <- 'ODC-BY 1.0'
licenses[licenses$license == "https://creativecommons.org/licenses/by-nc/3.0/",]$license <- 'CC-BY-NC 3.0'
licenses[licenses$license == "https://www.apache.org/licenses/LICENSE-2.0.html",]$license <- 'Apache'

licenses %>%
  select(source,license) %>%
  dplyr::count(source,license)

licenses.count <- licenses %>%
 #filter(license!="CC-BY-4.0") %>%
  dplyr::count(license) %>%
  arrange(n) %>%
  mutate(license=factor(license,license))

licenses.count
p <- licenses.count %>%
  ggplot(aes(x=license,y=n)) +
  geom_segment(
    aes(x=license,xend=license,y=0,yend=n),
    color=ifelse(licenses.count$license %in% c('CC0-1.0', 'CC-BY-4.0'), "orange", "grey"),
    size=ifelse(licenses.count$license %in% c('CC0-1.0', 'CC-BY-4.0'), 1.3, 1.0)
  ) +
  geom_point(
    color=ifelse(licenses.count$license %in% c('CC0-1.0', 'CC-BY-4.0'), "orange", "grey"),
    size=ifelse(licenses.count$license %in% c('CC0-1.0', 'CC-BY-4.0'), 5, 2)
  ) +
  theme_ipsum(
    base_family = 'Arial'
  ) +
  coord_flip() +
  theme(
    legend.position="none"
  ) +
  xlab("License") +
  ylab("Number of datasets") +
  ggtitle("Dataset Licenses") +
  ggplot2::annotate("text", x=grep("Other-Open", licenses.count$license), y=licenses.count$n[which(licenses.count=="Other-Open")]*2.5,
                    label=paste(licenses.count$n[which(licenses.count=="Other-Open")], 'datasets'),
                    color="grey", size=6,angle=0,fontface="bold", hjust=0) +
  ggplot2::annotate("text", x=grep("In Copyright", licenses.count$license), y=licenses.count$n[which(licenses.count=="In Copyright")]+100,
                    label=paste(licenses.count$n[which(licenses.count=="In Copyright")], 'dataset!'),
                    color="grey", size=6,angle=0,fontface="bold", hjust=0)
p
ggsave('../results/licenses-types.png', p, dpi = 300)





# Datatypes
datatypes <- data.uniq%>%
  select(source, datatypes) %>%
  mutate(datatypes=stringr::str_split(datatypes, ";")) %>%
  unnest(datatypes)

datatypes <- datatypes %>%
  mutate(datatypes=stringr::str_split(datatypes, ";")) %>%
  unnest(datatypes)

datatypes$types <- stringr::str_split(datatypes$datatypes, pattern = '/', simplify = T)[,1]
datatypes$subtypes <- stringr::str_split(datatypes$datatypes, pattern = '/', simplify = T)[,2]

datatypes %>%
  filter(!is.na(datatypes) & datatypes!="application/octet-stream") %>%
  group_by(datatypes) %>%
  dplyr::count(datatypes) %>%
  ungroup() %>%
  arrange(desc(n))


subtypes <- datatypes %>%
  filter(!is.na(datatypes) & datatypes!="application/octet-stream") %>%
  group_by(subtypes) %>%
  dplyr::count(subtypes) %>%
  ungroup() %>%
  arrange(desc(n))
subtypes

subtypes[subtypes$subtypes=='vnd.ms-excel',]$subtypes <- 'Microsoft Excel'
subtypes[subtypes$subtypes=='tab-separated-values',]$subtypes <- 'tsv'
subtypes[subtypes$subtypes=='vnd.openxmlformats-officedocument.wordprocessingml.document',]$subtypes <- 'Microsoft Word (OpenXML)'
subtypes[subtypes$subtypes=='vnd.openxmlformats-officedocument.spreadsheetml.sheet',]$subtypes <- 'Microsoft Excel (OpenXML)'
subtypes[subtypes$subtypes=='vnd.openxmlformats-officedocument.presentationml.presentation',]$subtypes <- 'Microsoft PorwerPoint (OpenXML)'

subtypes <- subtypes %>%
  dplyr::add_count(wt = n, name = 'total') %>%
  mutate(perc=round(n/total*100,2)) %>%
  select(-total)

subtypes %>% top_n(20) %>% summarise(sum(perc))

subtypes <- subtypes %>%
  mutate(subtypes=factor(subtypes,subtypes))

subtypes.top <- subtypes %>% top_n(20)

p <- subtypes.top %>%
  ggplot(aes(x=subtypes,y=n)) +
  geom_segment(
    aes(x=subtypes,xend=subtypes,y=0,yend=n),
    color=ifelse(!subtypes.top$subtypes %in% c("Microsoft Excel"), "orange", "grey"),
    size=ifelse(!subtypes.top$subtypes %in% c("Microsoft Excel"), 1.3, 0.7)
  ) +
  geom_point(
    color=ifelse(!subtypes.top$subtypes %in% c("Microsoft Excel"), "orange", "grey"),
    size=ifelse(!subtypes.top$subtypes %in% c("Microsoft Excel"), 5, 2)
  ) +
  ggtitle("Ten most common file formats in datasets") +
  theme_ipsum(
    base_family = 'Arial'
  ) +
  coord_flip() +
  theme(
    legend.position="none",
    plot.title = element_text(hjust = 0.9)
  ) +
  xlab("File Format") +
  ylab("Number of files")
p
ggsave('result../top20-file-formats.png', p, dpi = 300)


subtypes <- datatypes %>%
  filter(!is.na(datatypes) & datatypes!="application/octet-stream") %>%
  filter(source!="figshare") %>%
  group_by(subtypes) %>%
  dplyr::count(subtypes) %>%
  ungroup() %>%
  arrange(desc(n))
subtypes

subtypes[subtypes$subtypes=='vnd.ms-excel',]$subtypes <- 'Microsoft Excel'
subtypes[subtypes$subtypes=='tab-separated-values',]$subtypes <- 'tsv'
subtypes[subtypes$subtypes=='vnd.openxmlformats-officedocument.wordprocessingml.document',]$subtypes <- 'Microsoft Word (OpenXML)'
subtypes[subtypes$subtypes=='vnd.openxmlformats-officedocument.spreadsheetml.sheet',]$subtypes <- 'Microsoft Excel (OpenXML)'
subtypes[subtypes$subtypes=='vnd.openxmlformats-officedocument.presentationml.presentation',]$subtypes <- 'Microsoft PorwerPoint (OpenXML)'

subtypes <- subtypes %>%
  dplyr::add_count(wt = n, name = 'total') %>%
  mutate(perc=round(n/total*100,2)) %>%
  select(-total)

subtypes %>% top_n(20) %>% summarise(sum(perc))

subtypes <- subtypes %>%
  mutate(subtypes=factor(subtypes,subtypes))

subtypes.top <- subtypes
p <- subtypes.top %>%
  ggplot(aes(x=subtypes,y=n)) +
  geom_segment(
    aes(x=subtypes,xend=subtypes,y=0,yend=n),
    color="orange",
    size=1.3
  ) +
  geom_point(
    color="orange",
    size=5
  ) +
  ggtitle("File formats in datasets excluding Figshare") +
  theme_ipsum(
    base_family = 'Arial'
  ) +
  coord_flip() +
  theme(
    legend.position="none",
    plot.title = element_text(hjust = 0.9)
  ) +
  xlab("File Format") +
  ylab("Number of files")
p
ggsave('result../file-formats-wo-figshare.png', p, dpi = 300)


#Keywords
keywords <- data.uniq %>%
  mutate(keywords_set=stringr::str_split(keywords, "\\s*[;,]\\s*")) %>%
  unnest(keywords_set) %>%
  select(keywords_set)

interaction_kw <- keywords %>%
  filter(keywords_set != "") %>%
  mutate(keywords_set=str_to_lower(keywords_set)) %>%
  mutate(keywords_set=str_replace_all(keywords_set, "interactions", "interaction")) %>%
  mutate(keywords_set=str_replace_all(keywords_set, "networks", "network")) %>%
  mutate(keywords_set=str_replace_all(keywords_set, "[-‒‐––]", " ")) %>%
  dplyr::count(keywords_set) %>%
  arrange(desc(n)) %>%
  ungroup()

interaction_kw_top20 <- interaction_kw %>% slice_max(n=20, order_by = n)
interaction_kw_top20
plt <- interaction_kw_top20 %>%
  mutate(keywords_set=forcats::fct_reorder(keywords_set, n)) %>%
  ggplot() +
  geom_col(aes(n,keywords_set), fill='#076fa2', width=0.8) +
  scale_x_continuous(
    #limits=c(0, 86700), #TODO: fix limits
    #breaks = seq(0,max(metrics$value),1000), #TODO: fix breaks
    expand = c(0, 0),
    position = 'top',
    labels = scales::label_number_si()
  ) +
  scale_y_discrete(
    expand = expansion(add=c(0,0.5))
  ) +
  theme(
    panel.background = element_rect(fill="white"),
    panel.grid.major.x = element_line(color='#A8BAC4', linewidth = 0.3),
    axis.ticks.length = unit(0, 'mm'),
    axis.title = element_blank(),
    axis.line.y.left = element_line(color='black'),
    axis.text.y = element_blank(),
    axis.text.x = element_text(family = 'Econ Sans Cnd', size = 10)
  ) +
  geom_shadowtext(
    data =  subset(interaction_kw_top20, n < 1200),
    aes(n, y = keywords_set, label = keywords_set),
    hjust = 0,
    nudge_x = 0.3,
    colour = '#076fa2',
    bg.colour = 'white',
    bg.r = 0.2,
    family = 'Econ Sans Cnd',
    size = 3
  ) + geom_text(
    data = subset(interaction_kw_top20, n >= 1200),
    aes(30,y=keywords_set,label=keywords_set),
    hjust = 0,
    nudge_x = 0.3,
    colour = "white",
    family = 'Econ Sans Cnd',
    size = 3
  ) +
  labs(
    title="Most common dataset keywords",
    subtitle = 'Top 20 most frequent keywords'
  ) +
  theme(
    plot.title = element_text(
      family = 'Econ Sans Cnd',
      face = 'bold',
      size = 16
    ),
    plot.subtitle = element_text(
      family = 'Econ Sans Cnd',
      size=14
    )
  ) +
  theme(plot.margin = margin(0.01, 0.05, 0.02, 0.01, "npc"))
plt
ggsave('images/datasets-keywords.png', plt, dpi = 150)



# Has DOI
data.uniq %>%
  filter(stringr::str_detect(id, "^10\\.") | stringr::str_detect(id, "^doi:")) %>%
  dplyr::count() %>%
  pull(n)/nrow(data.uniq)*100
