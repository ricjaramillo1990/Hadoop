# Hadoop


## Sentiment Analysis

The task in completing this assignment is to analyse a range of reviews for the most common words
that appear for both positive and negative sentiments. The data are contained in a file called
sentiments.txt. The file contains the type of item
being reviewed (Restaurant, Movie, Product) followed by the review text and then a sentiment value
(1 for positive, 0 for negative). Each review is on a single line of the file with the different fields
separated by a tab character, as shown in the following example:

Restaurant I swung in to give them a try but was disappointed. 0 <br/>
Restaurant I had a pretty satisfying experience. 1 <br/>
Movie Some applause should be given to the "prelude". 1 <br/>
Product A must study for anyone interested poor design. 0 <br/>

Your task is to write a Java Hadoop Map/Reduce solution that will, in a single pass, find the 5 most
common words associated with a given item type and sentiment. The result will be 6 rows of data
consisting of the top 5 words for each item type and sentiment score in a form similar to the
following (the format and order can be different but the words for each item/sentiment must be
correct)
