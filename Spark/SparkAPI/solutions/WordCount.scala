// Load the input file moby_dick
val inputFile = textFile("moby_dick.txt")

// Use map and reduce to get a data set of word counts
// (key, value) pair, 1 for each word occurrence
// reduce with + to add up 1’s for each key (word)
val counts = sc.textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

// Save word count dataset to a file
counts.saveAsTextFile("word_count.txt")