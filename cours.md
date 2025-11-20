🎯 Étape 1 – config.py : pourquoi, comment, rôle dans un pipeline ?
🔵 Pourquoi un fichier config.py ?

Dans un vrai pipeline, on manipule beaucoup de chemins, paramètres, options, noms de fichiers, etc.

Sans fichier de config :

tu dupliques les chemins partout

si tu changes l’arborescence → tu dois tout modifier

ton orchestrateur devient un gros “spaghetti”

Avec config.py :

tous les chemins sont dans un seul endroit

on sépare logique & configuration

modularité → on peut réutiliser les fonctions sans tout casser

clarté → chaque module lit dans config

C’est un vrai réflexe de data engineer ⭐

🔵 Où se place config.py ?

Tu l’as mis dans :

notebooks/pipeline/config.py

---

