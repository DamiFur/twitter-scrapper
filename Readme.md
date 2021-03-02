# Twitter-Scrapper

Twitter-Scrapper is a wrapper built in top of a modified version of the Tweepyrate library, made by @finiteautomata. This wrapper allows to retrieve tweets either using queries, lists of users or lists of tweet ids. It also allows loading of multiple app credentials enabeling the user to sum the rettrieve capacity of multiple developer accounts.

If queries are used, Twitter-scrapper launches workers that use the Search API and workers that use the Stream API to maximize the amount of tweets that can be retrieved (depending, of course, on the capacity of the apps).

Twitter-Scrapper automatically stores the retrieved tweets along with all the metadata information in a mongo collection