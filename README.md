# Apache Hadoop’s MapReduce to get insights from the Top Hits from Spotify 2000-2019 dataset

Firstly, 

    python file (namely "csv2tsv_mert.py") should be called within same folder with the <input filename>.csv . It will produce the <input filename>.tsv file
    
        - python3 csv2tsv_mert.py <input filename>

Secondly,

    for putting <input filename>.tsv:

        - hadoop dfs -mkdir /input

        - hadoop dfs -put <input filename>.tsv /input

Thirdly,

    for compilation:

        - hadoop com.sun.tools.javac.Main∗.java       should be called.

        - jar cf Hw3.jar ∗.class                      should be called.

Finally,
    hadoop jar Hw3.jar Hw3 total /input outputtotal

    hadoop jar Hw3.jar Hw3 average /input outputaverage

    hadoop jar Hw3.jar Hw3 popular /input outputpopular

    hadoop jar Hw3.jar Hw3 explicitlypopular /input outputexplicitlypopular

    hadoop jar Hw3.jar Hw3 dancebyyear /input outputdancebyyear
    
    these methods will be called to produce outputs.
