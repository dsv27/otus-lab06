Запуск JSONReader

spark-submit --master local[*] --class com.example.JsonReader \
    /home/dsv/Prj/otus06/target/scala-2.11/otus-lab06-assembly-0.1.jar \
    /home/dsv/Prj/otus06/files/winemag-data-130k-v2.json


Запуск BostonCrimesMap

spark-submit --master local[*] --class com.example.BostonCrimesMap \
    /home/dsv/Prj/otus06/target/scala-2.11/otus-lab06-assembly-0.1.jar \
    /home/dsv/Prj/otus06/files/crime.csv \
    /home/dsv/Prj/otus06/files/offense_codes.csv \
    /home/dsv/Prj/otus06/files
