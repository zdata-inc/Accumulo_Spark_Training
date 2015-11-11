# Load the input file moby_dick
file = sc.textFile("moby_dick.txt")

# Use map and reduce to get a data set of word counts
# (key, value) pair, 1 for each word occurrence
# reduce with + to add up 1’s for each key (word)
wc = file.flatMap(lambda l: l.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda x,y: x + y)

# Save to a data file
wc.saveAsTextFile("word_count.txt")