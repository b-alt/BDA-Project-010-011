{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "name": "Untitled24.ipynb",
      "provenance": []
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "code",
      "execution_count": 1,
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "2CCoqK9-cQmn",
        "outputId": "1a501a2a-d5fc-4702-af9d-09494b498934"
      },
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Looking in indexes: https://pypi.org/simple, https://us-python.pkg.dev/colab-wheels/public/simple/\n",
            "Collecting pyspark\n",
            "  Downloading pyspark-3.3.0.tar.gz (281.3 MB)\n",
            "\u001b[K     |████████████████████████████████| 281.3 MB 45 kB/s \n",
            "\u001b[?25hCollecting py4j==0.10.9.5\n",
            "  Downloading py4j-0.10.9.5-py2.py3-none-any.whl (199 kB)\n",
            "\u001b[K     |████████████████████████████████| 199 kB 49.0 MB/s \n",
            "\u001b[?25hBuilding wheels for collected packages: pyspark\n",
            "  Building wheel for pyspark (setup.py) ... \u001b[?25l\u001b[?25hdone\n",
            "  Created wheel for pyspark: filename=pyspark-3.3.0-py2.py3-none-any.whl size=281764026 sha256=b3bcc166253443dc84418d1a1a962e6fb831d5eaca2d33a9b204cfcab55120fa\n",
            "  Stored in directory: /root/.cache/pip/wheels/7a/8e/1b/f73a52650d2e5f337708d9f6a1750d451a7349a867f928b885\n",
            "Successfully built pyspark\n",
            "Installing collected packages: py4j, pyspark\n",
            "Successfully installed py4j-0.10.9.5 pyspark-3.3.0\n"
          ]
        }
      ],
      "source": [
        "pip install pyspark"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql import SQLContext\n",
        "from pyspark import SparkContext\n",
        "from pyspark.sql import SparkSession\n",
        "sc =SparkContext()\n",
        "sqlContext = SQLContext(sc)\n",
        "spark = SparkSession.builder.appName(\"BCrimeClass\").getOrCreate()\n",
        "data = spark.read.csv('/content/train.csv',header=True)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "luzXezMsce-V",
        "outputId": "8c2e90f8-c2f6-4329-bf67-71ab3d8dc6a9"
      },
      "execution_count": 2,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stderr",
          "text": [
            "/usr/local/lib/python3.7/dist-packages/pyspark/sql/context.py:114: FutureWarning: Deprecated in 3.0.0. Use SparkSession.builder.getOrCreate() instead.\n",
            "  FutureWarning,\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "drop_list = ['Category','Descript', 'Resolution']\n",
        "data = data.select([column for column in data.columns if column not in drop_list])\n",
        "data.show(5)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "urkdhKvHcfzT",
        "outputId": "e29d0d5d-7b09-49c1-8116-12331b3bad2d"
      },
      "execution_count": 3,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+-------------------+---------+----------+--------------------+-------------------+------------------+\n",
            "|              Dates|DayOfWeek|PdDistrict|             Address|                  X|                 Y|\n",
            "+-------------------+---------+----------+--------------------+-------------------+------------------+\n",
            "|2015-05-13 23:53:00|Wednesday|  NORTHERN|  OAK ST / LAGUNA ST|  -122.425891675136|  37.7745985956747|\n",
            "|2015-05-13 23:53:00|Wednesday|  NORTHERN|  OAK ST / LAGUNA ST|  -122.425891675136|  37.7745985956747|\n",
            "|2015-05-13 23:33:00|Wednesday|  NORTHERN|VANNESS AV / GREE...|   -122.42436302145|  37.8004143219856|\n",
            "|2015-05-13 23:30:00|Wednesday|  NORTHERN|1500 Block of LOM...|-122.42699532676599| 37.80087263276921|\n",
            "|2015-05-13 23:30:00|Wednesday|      PARK|100 Block of BROD...|  -122.438737622757|37.771541172057795|\n",
            "+-------------------+---------+----------+--------------------+-------------------+------------------+\n",
            "only showing top 5 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "data.printSchema()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "ycL7p1nkcjcI",
        "outputId": "5161965f-2054-44d0-bb6d-48ece0373fd3"
      },
      "execution_count": 4,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "root\n",
            " |-- Dates: string (nullable = true)\n",
            " |-- DayOfWeek: string (nullable = true)\n",
            " |-- PdDistrict: string (nullable = true)\n",
            " |-- Address: string (nullable = true)\n",
            " |-- X: string (nullable = true)\n",
            " |-- Y: string (nullable = true)\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql.functions import col\n",
        "data.groupBy(\"Address\") \\\n",
        "    .count() \\\n",
        "    .orderBy(col(\"count\").desc()) \\\n",
        "    .show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "NXiVDp8xclwp",
        "outputId": "ebd51bb4-ad99-4c5e-f73e-b85d85362b47"
      },
      "execution_count": 5,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------------+-----+\n",
            "|             Address|count|\n",
            "+--------------------+-----+\n",
            "|800 Block of BRYA...|26533|\n",
            "|800 Block of MARK...| 6581|\n",
            "|2000 Block of MIS...| 5097|\n",
            "|1000 Block of POT...| 4063|\n",
            "|900 Block of MARK...| 3251|\n",
            "|  0 Block of TURK ST| 3228|\n",
            "|   0 Block of 6TH ST| 2884|\n",
            "|300 Block of ELLI...| 2703|\n",
            "|400 Block of ELLI...| 2590|\n",
            "|16TH ST / MISSION ST| 2504|\n",
            "|1000 Block of MAR...| 2489|\n",
            "|1100 Block of MAR...| 2319|\n",
            "|2000 Block of MAR...| 2168|\n",
            "|100 Block of OFAR...| 2140|\n",
            "|700 Block of MARK...| 2081|\n",
            "|3200 Block of 20T...| 2035|\n",
            "| 100 Block of 6TH ST| 1887|\n",
            "|500 Block of JOHN...| 1824|\n",
            "| TURK ST / TAYLOR ST| 1810|\n",
            "|200 Block of TURK ST| 1800|\n",
            "+--------------------+-----+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "data.groupBy(\"PdDistrict\") \\\n",
        "    .count() \\\n",
        "    .orderBy(col(\"count\").desc()) \\\n",
        "    .show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "SPArLpNCcpO0",
        "outputId": "8b035db3-62a7-4b34-d89b-9fbcd66ae52e"
      },
      "execution_count": 6,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+----------+------+\n",
            "|PdDistrict| count|\n",
            "+----------+------+\n",
            "|  SOUTHERN|157182|\n",
            "|   MISSION|119908|\n",
            "|  NORTHERN|105296|\n",
            "|   BAYVIEW| 89431|\n",
            "|   CENTRAL| 85460|\n",
            "|TENDERLOIN| 81809|\n",
            "| INGLESIDE| 78845|\n",
            "|   TARAVAL| 65596|\n",
            "|      PARK| 49313|\n",
            "|  RICHMOND| 45209|\n",
            "+----------+------+\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql.functions import isnull, when, count"
      ],
      "metadata": {
        "id": "XHP0q6k2crTI"
      },
      "execution_count": 7,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "data.select([count(when(isnull(c), c)).alias(c) for c in data.columns]).show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "_qMo5gP5cwMm",
        "outputId": "8de43256-cd57-4a87-9bb6-b2159931ef53"
      },
      "execution_count": 8,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+-----+---------+----------+-------+---+---+\n",
            "|Dates|DayOfWeek|PdDistrict|Address|  X|  Y|\n",
            "+-----+---------+----------+-------+---+---+\n",
            "|    0|        0|         0|      0|  0|  0|\n",
            "+-----+---------+----------+-------+---+---+\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.ml.feature import StringIndexer\n",
        "data = StringIndexer(\n",
        "    inputCol='DayOfWeek', \n",
        "    outputCol='a1', \n",
        "    handleInvalid='keep').fit(data).transform(data)\n",
        "data = StringIndexer(\n",
        "    inputCol='PdDistrict', \n",
        "    outputCol='a2', \n",
        "    handleInvalid='keep').fit(data).transform(data)\n",
        "data = StringIndexer(\n",
        "    inputCol='Address', \n",
        "    outputCol='a3', \n",
        "    handleInvalid='keep').fit(data).transform(data)\n",
        "data = StringIndexer(\n",
        "    inputCol='Dates', \n",
        "    outputCol='a4', \n",
        "    handleInvalid='keep').fit(data).transform(data) "
      ],
      "metadata": {
        "id": "oFRRQsjycyeI"
      },
      "execution_count": 9,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "data.show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "SeYPVO1ec8t_",
        "outputId": "92de45fd-f9dc-4a4a-a7aa-d5471354a341"
      },
      "execution_count": 10,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+-------------------+---------+----------+--------------------+-------------------+------------------+---+---+-------+--------+\n",
            "|              Dates|DayOfWeek|PdDistrict|             Address|                  X|                 Y| a1| a2|     a3|      a4|\n",
            "+-------------------+---------+----------+--------------------+-------------------+------------------+---+---+-------+--------+\n",
            "|2015-05-13 23:53:00|Wednesday|  NORTHERN|  OAK ST / LAGUNA ST|  -122.425891675136|  37.7745985956747|1.0|2.0| 4066.0|178757.0|\n",
            "|2015-05-13 23:53:00|Wednesday|  NORTHERN|  OAK ST / LAGUNA ST|  -122.425891675136|  37.7745985956747|1.0|2.0| 4066.0|178757.0|\n",
            "|2015-05-13 23:33:00|Wednesday|  NORTHERN|VANNESS AV / GREE...|   -122.42436302145|  37.8004143219856|1.0|2.0| 7793.0|389256.0|\n",
            "|2015-05-13 23:30:00|Wednesday|  NORTHERN|1500 Block of LOM...|-122.42699532676599| 37.80087263276921|1.0|2.0|  926.0| 41341.0|\n",
            "|2015-05-13 23:30:00|Wednesday|      PARK|100 Block of BROD...|  -122.438737622757|37.771541172057795|1.0|8.0| 3376.0| 41341.0|\n",
            "|2015-05-13 23:30:00|Wednesday| INGLESIDE| 0 Block of TEDDY AV|-122.40325236121201|   37.713430704116|1.0|6.0| 2485.0| 41341.0|\n",
            "|2015-05-13 23:30:00|Wednesday| INGLESIDE| AVALON AV / PERU AV|  -122.423326976668|  37.7251380403778|1.0|6.0|21708.0| 41341.0|\n",
            "|2015-05-13 23:30:00|Wednesday|   BAYVIEW|KIRKWOOD AV / DON...|  -122.371274317441|  37.7275640719518|1.0|3.0|17683.0| 41341.0|\n",
            "|2015-05-13 23:00:00|Wednesday|  RICHMOND|600 Block of 47TH AV|  -122.508194031117|37.776601260681204|1.0|9.0| 8254.0|178756.0|\n",
            "|2015-05-13 23:00:00|Wednesday|   CENTRAL|JEFFERSON ST / LE...|  -122.419087676747|  37.8078015516515|1.0|4.0| 6174.0|178756.0|\n",
            "|2015-05-13 22:58:00|Wednesday|   CENTRAL|JEFFERSON ST / LE...|  -122.419087676747|  37.8078015516515|1.0|4.0| 6174.0|389255.0|\n",
            "|2015-05-13 22:30:00|Wednesday|   TARAVAL|0 Block of ESCOLT...|  -122.487983072777|37.737666654332706|1.0|7.0|12892.0|178755.0|\n",
            "|2015-05-13 22:30:00|Wednesday|TENDERLOIN|  TURK ST / JONES ST|-122.41241426358101|  37.7830037964534|1.0|5.0|  151.0|178755.0|\n",
            "|2015-05-13 22:06:00|Wednesday|  NORTHERN|FILLMORE ST / GEA...|  -122.432914603494|  37.7843533426568|1.0|2.0|  492.0|389254.0|\n",
            "|2015-05-13 22:00:00|Wednesday|   BAYVIEW|200 Block of WILL...|  -122.397744427103|  37.7299346936044|1.0|3.0|  123.0|102225.0|\n",
            "|2015-05-13 22:00:00|Wednesday|   BAYVIEW|0 Block of MENDEL...|-122.38369150395901|  37.7431890419965|1.0|3.0| 6032.0|102225.0|\n",
            "|2015-05-13 22:00:00|Wednesday|TENDERLOIN|  EDDY ST / JONES ST|  -122.412597377187|37.783932027727296|1.0|5.0|  426.0|102225.0|\n",
            "|2015-05-13 21:55:00|Wednesday| INGLESIDE|GODEUS ST / MISSI...|  -122.421681531572|  37.7428222004845|1.0|6.0|20315.0|389253.0|\n",
            "|2015-05-13 21:40:00|Wednesday|   BAYVIEW|MENDELL ST / HUDS...|-122.38640086995301|   37.738983491072|1.0|3.0| 6004.0|389252.0|\n",
            "|2015-05-13 21:30:00|Wednesday|TENDERLOIN|100 Block of JONE...|  -122.412249767634|   37.782556330202|1.0|5.0|  100.0|178754.0|\n",
            "+-------------------+---------+----------+--------------------+-------------------+------------------+---+---+-------+--------+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "train1 = data.drop('PdDistrict','DayofWeek','Address','Dates')"
      ],
      "metadata": {
        "id": "tv6iXUjWhtcO"
      },
      "execution_count": 11,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "train1.show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "Z924itbbhyLE",
        "outputId": "17146f4e-9cd4-4cd3-e664-799ddd815b55"
      },
      "execution_count": 12,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+-------------------+------------------+---+---+-------+--------+\n",
            "|                  X|                 Y| a1| a2|     a3|      a4|\n",
            "+-------------------+------------------+---+---+-------+--------+\n",
            "|  -122.425891675136|  37.7745985956747|1.0|2.0| 4066.0|178757.0|\n",
            "|  -122.425891675136|  37.7745985956747|1.0|2.0| 4066.0|178757.0|\n",
            "|   -122.42436302145|  37.8004143219856|1.0|2.0| 7793.0|389256.0|\n",
            "|-122.42699532676599| 37.80087263276921|1.0|2.0|  926.0| 41341.0|\n",
            "|  -122.438737622757|37.771541172057795|1.0|8.0| 3376.0| 41341.0|\n",
            "|-122.40325236121201|   37.713430704116|1.0|6.0| 2485.0| 41341.0|\n",
            "|  -122.423326976668|  37.7251380403778|1.0|6.0|21708.0| 41341.0|\n",
            "|  -122.371274317441|  37.7275640719518|1.0|3.0|17683.0| 41341.0|\n",
            "|  -122.508194031117|37.776601260681204|1.0|9.0| 8254.0|178756.0|\n",
            "|  -122.419087676747|  37.8078015516515|1.0|4.0| 6174.0|178756.0|\n",
            "|  -122.419087676747|  37.8078015516515|1.0|4.0| 6174.0|389255.0|\n",
            "|  -122.487983072777|37.737666654332706|1.0|7.0|12892.0|178755.0|\n",
            "|-122.41241426358101|  37.7830037964534|1.0|5.0|  151.0|178755.0|\n",
            "|  -122.432914603494|  37.7843533426568|1.0|2.0|  492.0|389254.0|\n",
            "|  -122.397744427103|  37.7299346936044|1.0|3.0|  123.0|102225.0|\n",
            "|-122.38369150395901|  37.7431890419965|1.0|3.0| 6032.0|102225.0|\n",
            "|  -122.412597377187|37.783932027727296|1.0|5.0|  426.0|102225.0|\n",
            "|  -122.421681531572|  37.7428222004845|1.0|6.0|20315.0|389253.0|\n",
            "|-122.38640086995301|   37.738983491072|1.0|3.0| 6004.0|389252.0|\n",
            "|  -122.412249767634|   37.782556330202|1.0|5.0|  100.0|178754.0|\n",
            "+-------------------+------------------+---+---+-------+--------+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "train1=train1.withColumnRenamed(\"a1\", \"DayOfWeek\")\n",
        "train1=train1.withColumnRenamed(\"a2\", \"PdDistrict\")\n",
        "train1=train1.withColumnRenamed(\"a3\", \"Address\")\n",
        "train1=train1.withColumnRenamed(\"a4\", \"Dates\")"
      ],
      "metadata": {
        "id": "qYs7H5mziJKm"
      },
      "execution_count": 13,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "train1.show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "WdO5jslbixC4",
        "outputId": "c362a74c-4498-4201-a096-4b7eeb5047c3"
      },
      "execution_count": 14,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+-------------------+------------------+---------+----------+-------+--------+\n",
            "|                  X|                 Y|DayOfWeek|PdDistrict|Address|   Dates|\n",
            "+-------------------+------------------+---------+----------+-------+--------+\n",
            "|  -122.425891675136|  37.7745985956747|      1.0|       2.0| 4066.0|178757.0|\n",
            "|  -122.425891675136|  37.7745985956747|      1.0|       2.0| 4066.0|178757.0|\n",
            "|   -122.42436302145|  37.8004143219856|      1.0|       2.0| 7793.0|389256.0|\n",
            "|-122.42699532676599| 37.80087263276921|      1.0|       2.0|  926.0| 41341.0|\n",
            "|  -122.438737622757|37.771541172057795|      1.0|       8.0| 3376.0| 41341.0|\n",
            "|-122.40325236121201|   37.713430704116|      1.0|       6.0| 2485.0| 41341.0|\n",
            "|  -122.423326976668|  37.7251380403778|      1.0|       6.0|21708.0| 41341.0|\n",
            "|  -122.371274317441|  37.7275640719518|      1.0|       3.0|17683.0| 41341.0|\n",
            "|  -122.508194031117|37.776601260681204|      1.0|       9.0| 8254.0|178756.0|\n",
            "|  -122.419087676747|  37.8078015516515|      1.0|       4.0| 6174.0|178756.0|\n",
            "|  -122.419087676747|  37.8078015516515|      1.0|       4.0| 6174.0|389255.0|\n",
            "|  -122.487983072777|37.737666654332706|      1.0|       7.0|12892.0|178755.0|\n",
            "|-122.41241426358101|  37.7830037964534|      1.0|       5.0|  151.0|178755.0|\n",
            "|  -122.432914603494|  37.7843533426568|      1.0|       2.0|  492.0|389254.0|\n",
            "|  -122.397744427103|  37.7299346936044|      1.0|       3.0|  123.0|102225.0|\n",
            "|-122.38369150395901|  37.7431890419965|      1.0|       3.0| 6032.0|102225.0|\n",
            "|  -122.412597377187|37.783932027727296|      1.0|       5.0|  426.0|102225.0|\n",
            "|  -122.421681531572|  37.7428222004845|      1.0|       6.0|20315.0|389253.0|\n",
            "|-122.38640086995301|   37.738983491072|      1.0|       3.0| 6004.0|389252.0|\n",
            "|  -122.412249767634|   37.782556330202|      1.0|       5.0|  100.0|178754.0|\n",
            "+-------------------+------------------+---------+----------+-------+--------+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "(trainingData, testData) = train1.randomSplit([0.7, 0.3], seed = 100)\n",
        "print(\"Training Dataset Count: \" + str(trainingData.count()))\n",
        "print(\"Test Dataset Count: \" + str(testData.count()))"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "dmvgWJR3i8lH",
        "outputId": "6b10762a-5749-4f3e-e141-a2b53f430a55"
      },
      "execution_count": 17,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Training Dataset Count: 614485\n",
            "Test Dataset Count: 263564\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.ml.classification import NaiveBayes\n",
        "nb = NaiveBayes(smoothing=1)\n",
        "model = nb.fit(trainingData)\n",
        "predictions = model.transform(testData)\n",
        "predictions.filter(predictions['prediction'] == 0) \\\n",
        "    .select(\"X\",\"Y\",\"probability\",\"PdDistrict\",\"DayofWeek\",\"Address\",\"Dates\",\"prediction\") \\\n",
        "    .orderBy(\"probability\", ascending=False) \\\n",
        "    .show(n = 10, truncate = 30)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 380
        },
        "id": "GHY64vdci2gX",
        "outputId": "5ffaecea-77f1-4bd3-eb55-2389e29d97db"
      },
      "execution_count": 18,
      "outputs": [
        {
          "output_type": "error",
          "ename": "IllegalArgumentException",
          "evalue": "ignored",
          "traceback": [
            "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
            "\u001b[0;31mIllegalArgumentException\u001b[0m                  Traceback (most recent call last)",
            "\u001b[0;32m<ipython-input-18-85606011acd3>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m\u001b[0m\n\u001b[1;32m      1\u001b[0m \u001b[0;32mfrom\u001b[0m \u001b[0mpyspark\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mml\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mclassification\u001b[0m \u001b[0;32mimport\u001b[0m \u001b[0mNaiveBayes\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m      2\u001b[0m \u001b[0mnb\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mNaiveBayes\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0msmoothing\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0;36m1\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m----> 3\u001b[0;31m \u001b[0mmodel\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mnb\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mfit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mtrainingData\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m      4\u001b[0m \u001b[0mpredictions\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mmodel\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mtransform\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mtestData\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m      5\u001b[0m \u001b[0mpredictions\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mfilter\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mpredictions\u001b[0m\u001b[0;34m[\u001b[0m\u001b[0;34m'prediction'\u001b[0m\u001b[0;34m]\u001b[0m \u001b[0;34m==\u001b[0m \u001b[0;36m0\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;31m \u001b[0m\u001b[0;31m\\\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
            "\u001b[0;32m/usr/local/lib/python3.7/dist-packages/pyspark/ml/base.py\u001b[0m in \u001b[0;36mfit\u001b[0;34m(self, dataset, params)\u001b[0m\n\u001b[1;32m    203\u001b[0m                 \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mcopy\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mparams\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_fit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mdataset\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    204\u001b[0m             \u001b[0;32melse\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 205\u001b[0;31m                 \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_fit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mdataset\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    206\u001b[0m         \u001b[0;32melse\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    207\u001b[0m             raise TypeError(\n",
            "\u001b[0;32m/usr/local/lib/python3.7/dist-packages/pyspark/ml/wrapper.py\u001b[0m in \u001b[0;36m_fit\u001b[0;34m(self, dataset)\u001b[0m\n\u001b[1;32m    377\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    378\u001b[0m     \u001b[0;32mdef\u001b[0m \u001b[0m_fit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mself\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mdataset\u001b[0m\u001b[0;34m:\u001b[0m \u001b[0mDataFrame\u001b[0m\u001b[0;34m)\u001b[0m \u001b[0;34m->\u001b[0m \u001b[0mJM\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 379\u001b[0;31m         \u001b[0mjava_model\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_fit_java\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mdataset\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    380\u001b[0m         \u001b[0mmodel\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_create_model\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mjava_model\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    381\u001b[0m         \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_copyValues\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mmodel\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
            "\u001b[0;32m/usr/local/lib/python3.7/dist-packages/pyspark/ml/wrapper.py\u001b[0m in \u001b[0;36m_fit_java\u001b[0;34m(self, dataset)\u001b[0m\n\u001b[1;32m    374\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    375\u001b[0m         \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_transfer_params_to_java\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 376\u001b[0;31m         \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_java_obj\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mfit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mdataset\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_jdf\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    377\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    378\u001b[0m     \u001b[0;32mdef\u001b[0m \u001b[0m_fit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mself\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mdataset\u001b[0m\u001b[0;34m:\u001b[0m \u001b[0mDataFrame\u001b[0m\u001b[0;34m)\u001b[0m \u001b[0;34m->\u001b[0m \u001b[0mJM\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
            "\u001b[0;32m/usr/local/lib/python3.7/dist-packages/py4j/java_gateway.py\u001b[0m in \u001b[0;36m__call__\u001b[0;34m(self, *args)\u001b[0m\n\u001b[1;32m   1320\u001b[0m         \u001b[0manswer\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mgateway_client\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0msend_command\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mcommand\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m   1321\u001b[0m         return_value = get_return_value(\n\u001b[0;32m-> 1322\u001b[0;31m             answer, self.gateway_client, self.target_id, self.name)\n\u001b[0m\u001b[1;32m   1323\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m   1324\u001b[0m         \u001b[0;32mfor\u001b[0m \u001b[0mtemp_arg\u001b[0m \u001b[0;32min\u001b[0m \u001b[0mtemp_args\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
            "\u001b[0;32m/usr/local/lib/python3.7/dist-packages/pyspark/sql/utils.py\u001b[0m in \u001b[0;36mdeco\u001b[0;34m(*a, **kw)\u001b[0m\n\u001b[1;32m    194\u001b[0m                 \u001b[0;31m# Hide where the exception came from that shows a non-Pythonic\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    195\u001b[0m                 \u001b[0;31m# JVM exception message.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 196\u001b[0;31m                 \u001b[0;32mraise\u001b[0m \u001b[0mconverted\u001b[0m \u001b[0;32mfrom\u001b[0m \u001b[0;32mNone\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    197\u001b[0m             \u001b[0;32melse\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    198\u001b[0m                 \u001b[0;32mraise\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
            "\u001b[0;31mIllegalArgumentException\u001b[0m: features does not exist. Available: X, Y, DayOfWeek, PdDistrict, Address, Dates"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.ml.classification import RandomForestClassifier\n",
        "rf = RandomForestClassifier(labelCol=\"label\", \\\n",
        "                            featuresCol=\"features\", \\\n",
        "                            numTrees = 100, \\\n",
        "                            maxDepth = 4, \\\n",
        "                            maxBins = 32)\n",
        "# Train model with Training Data\n",
        "rfModel = rf.fit(trainingData)\n",
        "predictions = rfModel.transform(testData)\n",
        "predictions.filter(predictions['prediction'] == 0) \\\n",
        "    .select(\"X\",\"Y\",\"probability\",\"PdDistrict\",\"DayofWeek\",\"Address\",\"Dates\",\"prediction\") \\\n",
        "    .orderBy(\"probability\", ascending=False) \\\n",
        "    .show(n = 10, truncate = 30)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 380
        },
        "id": "bIGNMdkHkZsS",
        "outputId": "2d65ccc6-1dfe-4e07-ee20-652715b1b8c8"
      },
      "execution_count": 19,
      "outputs": [
        {
          "output_type": "error",
          "ename": "IllegalArgumentException",
          "evalue": "ignored",
          "traceback": [
            "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
            "\u001b[0;31mIllegalArgumentException\u001b[0m                  Traceback (most recent call last)",
            "\u001b[0;32m<ipython-input-19-f771eeb22b3f>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m\u001b[0m\n\u001b[1;32m      6\u001b[0m                             maxBins = 32)\n\u001b[1;32m      7\u001b[0m \u001b[0;31m# Train model with Training Data\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m----> 8\u001b[0;31m \u001b[0mrfModel\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mrf\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mfit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mtrainingData\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m      9\u001b[0m \u001b[0mpredictions\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mrfModel\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mtransform\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mtestData\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     10\u001b[0m \u001b[0mpredictions\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mfilter\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mpredictions\u001b[0m\u001b[0;34m[\u001b[0m\u001b[0;34m'prediction'\u001b[0m\u001b[0;34m]\u001b[0m \u001b[0;34m==\u001b[0m \u001b[0;36m0\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;31m \u001b[0m\u001b[0;31m\\\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
            "\u001b[0;32m/usr/local/lib/python3.7/dist-packages/pyspark/ml/base.py\u001b[0m in \u001b[0;36mfit\u001b[0;34m(self, dataset, params)\u001b[0m\n\u001b[1;32m    203\u001b[0m                 \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mcopy\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mparams\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_fit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mdataset\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    204\u001b[0m             \u001b[0;32melse\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 205\u001b[0;31m                 \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_fit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mdataset\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    206\u001b[0m         \u001b[0;32melse\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    207\u001b[0m             raise TypeError(\n",
            "\u001b[0;32m/usr/local/lib/python3.7/dist-packages/pyspark/ml/wrapper.py\u001b[0m in \u001b[0;36m_fit\u001b[0;34m(self, dataset)\u001b[0m\n\u001b[1;32m    377\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    378\u001b[0m     \u001b[0;32mdef\u001b[0m \u001b[0m_fit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mself\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mdataset\u001b[0m\u001b[0;34m:\u001b[0m \u001b[0mDataFrame\u001b[0m\u001b[0;34m)\u001b[0m \u001b[0;34m->\u001b[0m \u001b[0mJM\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 379\u001b[0;31m         \u001b[0mjava_model\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_fit_java\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mdataset\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    380\u001b[0m         \u001b[0mmodel\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_create_model\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mjava_model\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    381\u001b[0m         \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_copyValues\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mmodel\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
            "\u001b[0;32m/usr/local/lib/python3.7/dist-packages/pyspark/ml/wrapper.py\u001b[0m in \u001b[0;36m_fit_java\u001b[0;34m(self, dataset)\u001b[0m\n\u001b[1;32m    374\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    375\u001b[0m         \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_transfer_params_to_java\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 376\u001b[0;31m         \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_java_obj\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mfit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mdataset\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_jdf\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    377\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    378\u001b[0m     \u001b[0;32mdef\u001b[0m \u001b[0m_fit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mself\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mdataset\u001b[0m\u001b[0;34m:\u001b[0m \u001b[0mDataFrame\u001b[0m\u001b[0;34m)\u001b[0m \u001b[0;34m->\u001b[0m \u001b[0mJM\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
            "\u001b[0;32m/usr/local/lib/python3.7/dist-packages/py4j/java_gateway.py\u001b[0m in \u001b[0;36m__call__\u001b[0;34m(self, *args)\u001b[0m\n\u001b[1;32m   1320\u001b[0m         \u001b[0manswer\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mgateway_client\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0msend_command\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mcommand\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m   1321\u001b[0m         return_value = get_return_value(\n\u001b[0;32m-> 1322\u001b[0;31m             answer, self.gateway_client, self.target_id, self.name)\n\u001b[0m\u001b[1;32m   1323\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m   1324\u001b[0m         \u001b[0;32mfor\u001b[0m \u001b[0mtemp_arg\u001b[0m \u001b[0;32min\u001b[0m \u001b[0mtemp_args\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
            "\u001b[0;32m/usr/local/lib/python3.7/dist-packages/pyspark/sql/utils.py\u001b[0m in \u001b[0;36mdeco\u001b[0;34m(*a, **kw)\u001b[0m\n\u001b[1;32m    194\u001b[0m                 \u001b[0;31m# Hide where the exception came from that shows a non-Pythonic\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    195\u001b[0m                 \u001b[0;31m# JVM exception message.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 196\u001b[0;31m                 \u001b[0;32mraise\u001b[0m \u001b[0mconverted\u001b[0m \u001b[0;32mfrom\u001b[0m \u001b[0;32mNone\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    197\u001b[0m             \u001b[0;32melse\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    198\u001b[0m                 \u001b[0;32mraise\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
            "\u001b[0;31mIllegalArgumentException\u001b[0m: features does not exist. Available: X, Y, DayOfWeek, PdDistrict, Address, Dates"
          ]
        }
      ]
    }
  ]
}