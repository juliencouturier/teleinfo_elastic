#Teleinfo compteur vers Elasticsearch
Le script lit les téléinformations du compteur électrique et envoie les information dans une base elasticsearch

## Packages requis
2 packages sont requis : 
* elasticsearch pour pouvoir indexer les informations
* pyserial pour pouvoir lire le port série

Il est possible de les installer avec `pip install -r requirements.txt`

## Exécution
Pour le moment tous les paramètres sont est en dur dans le script...

## A venir
Ajout d'un cache si le serveur ES n'est pas disponible afin de ne pas perdre d'informations

