
## Skapespeare example

**Step 1**: Before all, search "shakespeare.txt" on google, download first link and rename to skapespeare.txt.

**Step 2**: Load skakespeare complete works to spark context.


```python
lines = sc.textFile("/home/adrian/development/spark/notebooks/shakespeare/shakespeare.txt")
```

**Step 3**: Get words.


```python
words = lines.flatMap(lambda line: line.split(' ')) \
                .map(lambda word: word.lower()) \
                .filter(lambda word: len(word) > 0)
```

**Step 4**: Remove stop words and puctuation symbols.


```python
from string import punctuation

def strip_punctuation(string):
    return ''.join(char for char in string if char not in punctuation)

stop_words = ["i", "you", "he", "she", "it", "they", "we", "us", "them", "him", "her", "my", "your", "our", "his", "its", "their", "mine", "ours", "yours", "theirs", "hers", "me", "ii","iii","iv","vi","vii","viii","ix","x","xi","xii","xiii","xiv","xv","xvi","xvii","xvii","xviii","xix","xx", "one", "two", "thee", "four", "five", "six", "seven", "eight", "nine", "ten", "will", "thy", "a","able","about","above","abst","accordance","according","accordingly","across","act","actually","added","adj","affected","affecting","affects","after","afterwards","again","against","ah","all","almost","alone","along","already","also","although","always","am","among","amongst","an","and","announce","another","any","anybody","anyhow","anymore","anyone","anything","anyway","anyways","anywhere","apparently","approximately","are","aren","arent","arise","around","as","aside","ask","asking","at","auth","available","away","awfully","b","back","be","became","because","become","becomes","becoming","been","before","beforehand","begin","beginning","beginnings","begins","behind","being","believe","below","beside","besides","between","beyond","biol","both","brief","briefly","but","by","c","ca","came","can","cannot","can't","cause","causes","certain","certainly","co","com","come","comes","contain","containing","contains","could","couldnt","d","date","did","didn't","different","do","does","doesn't","doing","done","don't","down","downwards","due","during","e","each","ed","edu","effect","eg","eight","eighty","either","else","elsewhere","end","ending","enough","especially","et","et-al","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","except","f","far","few","ff","fifth","first","five","fix","followed","following","follows","for","former","formerly","forth","found","four","from","further","furthermore","g","gave","get","gets","getting","give","given","gives","giving","go","goes","gone","got","gotten","h","had","happens","hardly","has","hasn't","have","haven't","having","he","hed","hence","her","here","hereafter","hereby","herein","heres","hereupon","hers","herself","hes","hi","hid","him","himself","his","hither","home","how","howbeit","however","hundred","i","id","ie","if","i'll","im","immediate","immediately","importance","important","in","inc","indeed","index","information","instead","into","invention","inward","is","isn't","it","itd","it'll","its","itself","i've","j","just","k","keep	keeps","kept","kg","km","know","known","knows","l","largely","last","lately","later","latter","latterly","least","less","lest","let","lets","like","liked","likely","line","little","'ll","look","looking","looks","ltd","m","made","mainly","make","makes","many","may","maybe","me","mean","means","meantime","meanwhile","merely","mg","might","million","miss","ml","more","moreover","most","mostly","mr","mrs","much","mug","must","my","myself","n","na","name","namely","nay","nd","near","nearly","necessarily","necessary","need","needs","neither","never","nevertheless","new","next","nine","ninety","no","nobody","non","none","nonetheless","noone","nor","normally","nos","not","noted","nothing","now","nowhere","o","obtain","obtained","obviously","of","off","often","oh","ok","okay","old","omitted","on","once","one","ones","only","onto","or","ord","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","owing","own","p","page","pages","part","particular","particularly","past","per","perhaps","placed","please","plus","poorly","possible","possibly","potentially","pp","predominantly","present","previously","primarily","probably","promptly","proud","provides","put","q","que","quickly","quite","qv","r","ran","rather","rd","re","readily","really","recent","recently","ref","refs","regarding","regardless","regards","related","relatively","research","respectively","resulted","resulting","results","right","run","s","said","same","saw","say","saying","says","sec","section","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sent","seven","several","shall","she","shed","she'll","shes","should","shouldn't","show","showed","shown","showns","shows","significant","significantly","similar","similarly","since","six","slightly","so","some","somebody","somehow","someone","somethan","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specifically","specified","specify","specifying","still","stop","strongly","sub","substantially","successfully","such","sufficiently","suggest","sup","sure	t","take","taken","taking","tell","tends","th","than","thank","thanks","thanx","that","that'll","thats","that've","the","their","theirs","them","themselves","then","thence","there","thereafter","thereby","thered","therefore","therein","there'll","thereof","therere","theres","thereto","thereupon","there've","these","they","theyd","they'll","theyre","they've","think","this","those","thou","though","thoughh","thousand","throug","through","throughout","thru","thus","til","tip","to","together","too","took","toward","towards","tried","tries","truly","try","trying","ts","twice","two","u","un","under","unfortunately","unless","unlike","unlikely","until","unto","up","upon","ups","us","use","used","useful","usefully","usefulness","uses","using","usually","v","value","various","'ve","very","via","viz","vol","vols","vs","w","want","wants","was","wasnt","way","we","wed","welcome","we'll","went","were","werent","we've","what","whatever","what'll","whats","when","whence","whenever","where","whereafter","whereas","whereby","wherein","wheres","whereupon","wherever","whether","which","while","whim","whither","who","whod","whoever","whole","who'll","whom","whomever","whos","whose","why","widely","willing","wish","with","within","without","wont","words","world","would","wouldnt","www","x","y","yes","yet","you","youd","you'll","your","youre","yours","yourself","yourselves","you've","z","zero"]

words = words.map(lambda word: strip_punctuation(word).strip()) \
             .filter(lambda word: not (word in stop_words or word.isdigit())) \
             .cache()

print('Words: %s.' % ', '.join(words.take(10)))
```

    Words: sonnets, william, shakespeare, fairest, creatures, desire, increase, beautys, rose, die.


**Step 5**: Get more popular words.


```python
from string import punctuation

def sum():
    return lambda a,b: a + b

def descendent():
    return lambda word_times: -word_times[1]

words_frequency = words.map(lambda word: (word, 1)) \
                       .reduceByKey(sum()) \
                       .cache()

print('Words frequency: %s.' % words_frequency.takeOrdered(10, descendent()))
```

    Words frequency: [('lord', 3059), ('king', 2861), ('good', 2812), ('sir', 2754), ('well', 2462), ('enter', 2098), ('love', 2054), ('ill', 1972), ('hath', 1941), ('man', 1835)].


**Step 6**: Get diferent words count.


```python
print('Diferent words count: %s.' % str(words.distinct().count()))
```

    Diferent words count: 27441.


**Step 7**: Get most frequent two letters words.


```python
two_letter_words_frequency = words_frequency.filter(lambda word_times: len(word_times[0]) == 2)
                                            
print('Two letter words frequency: %s.' % two_letter_words_frequency.takeOrdered(10, descendent()))
```

    Two letter words frequency: [('ay', 764), ('ye', 289), ('ha', 217), ('ho', 195), ('em', 167), ('de', 115), ('la', 77), ('lo', 74), ('le', 60), ('je', 28)].

