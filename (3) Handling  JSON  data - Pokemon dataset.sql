#Contex : When we are dealing with data base, we often use No-SQL databse to ensure affordable cost and quick computation. Usually, No-SQL data is written in JSON, and many of us is not familiar with how to read and manipulate data with JSON. Therefore we will convert json_data to more friendly reading : schema-table.

#Business Objective : We want to convert JSON data into a Schmea Table using Queries.

#Data    : Pokemon Data 
#Source  : https://github.com/fanzeyi/pokemon.json/blob/master/pokedex.json
#Dialect : Google BigQuery SQL 
WITH
  json_data AS (
  SELECT
    '''[{
    "id": 1,
    "name": {
      "english": "Bulbasaur",
      "japanese": "フシギダネ",
      "chinese": "妙蛙种子",
      "french": "Bulbizarre"
    },
    "type": [
      "Grass",
      "Poison"
    ],
    "base": {
      "HP": 45,
      "Attack": 49,
      "Defense": 49,
      "Sp. Attack": 65,
      "Sp. Defense": 65,
      "Speed": 45
    }
  },
  {
    "id": 2,
    "name": {
      "english": "Ivysaur",
      "japanese": "フシギソウ",
      "chinese": "妙蛙草",
      "french": "Herbizarre"
    },
    "type": [
      "Grass",
      "Poison"
    ],
    "base": {
      "HP": 60,
      "Attack": 62,
      "Defense": 63,
      "Sp. Attack": 80,
      "Sp. Defense": 80,
      "Speed": 60
    }
  },
  {
    "id": 3,
    "name": {
      "english": "Venusaur",
      "japanese": "フシギバナ",
      "chinese": "妙蛙花",
      "french": "Florizarre"
    },
    "type": [
      "Grass",
      "Poison"
    ],
    "base": {
      "HP": 80,
      "Attack": 82,
      "Defense": 83,
      "Sp. Attack": 100,
      "Sp. Defense": 100,
      "Speed": 80
    }
  },
  {
    "id": 4,
    "name": {
      "english": "Charmander",
      "japanese": "ヒトカゲ",
      "chinese": "小火龙",
      "french": "Salamèche"
    },
    "type": [
      "Fire"
    ],
    "base": {
      "HP": 39,
      "Attack": 52,
      "Defense": 43,
      "Sp. Attack": 60,
      "Sp. Defense": 50,
      "Speed": 65
    }
  },
  {
    "id": 5,
    "name": {
      "english": "Charmeleon",
      "japanese": "リザード",
      "chinese": "火恐龙",
      "french": "Reptincel"
    },
    "type": [
      "Fire"
    ],
    "base": {
      "HP": 58,
      "Attack": 64,
      "Defense": 58,
      "Sp. Attack": 80,
      "Sp. Defense": 65,
      "Speed": 80
    }
  },
  {
    "id": 6,
    "name": {
      "english": "Charizard",
      "japanese": "リザードン",
      "chinese": "喷火龙",
      "french": "Dracaufeu"
    },
    "type": [
      "Fire",
      "Flying"
    ],
    "base": {
      "HP": 78,
      "Attack": 84,
      "Defense": 78,
      "Sp. Attack": 109,
      "Sp. Defense": 85,
      "Speed": 100
    }
  },
  {
    "id": 7,
    "name": {
      "english": "Squirtle",
      "japanese": "ゼニガメ",
      "chinese": "杰尼龟",
      "french": "Carapuce"
    },
    "type": [
      "Water"
    ],
    "base": {
      "HP": 44,
      "Attack": 48,
      "Defense": 65,
      "Sp. Attack": 50,
      "Sp. Defense": 64,
      "Speed": 43
    }
  },
  {
    "id": 8,
    "name": {
      "english": "Wartortle",
      "japanese": "カメール",
      "chinese": "卡咪龟",
      "french": "Carabaffe"
    },
    "type": [
      "Water"
    ],
    "base": {
      "HP": 59,
      "Attack": 63,
      "Defense": 80,
      "Sp. Attack": 65,
      "Sp. Defense": 80,
      "Speed": 58
    }
  },
  {
    "id": 9,
    "name": {
      "english": "Blastoise",
      "japanese": "カメックス",
      "chinese": "水箭龟",
      "french": "Tortank"
    },
    "type": [
      "Water"
    ],
    "base": {
      "HP": 79,
      "Attack": 83,
      "Defense": 100,
      "Sp. Attack": 85,
      "Sp. Defense": 105,
      "Speed": 78
    }
  },
  {
    "id": 10,
    "name": {
      "english": "Caterpie",
      "japanese": "キャタピー",
      "chinese": "绿毛虫",
      "french": "Chenipan"
    },
    "type": [
      "Bug"
    ],
    "base": {
      "HP": 45,
      "Attack": 30,
      "Defense": 35,
      "Sp. Attack": 20,
      "Sp. Defense": 20,
      "Speed": 45
    }
  },
  {
    "id": 809,
    "name": {
      "english": "Melmetal",
      "japanese": "メルメタル",
      "chinese": "美录梅塔",
      "french": ""
    },
    "type": [
      "Steel"
    ],
    "base": {
      "HP": 135,
      "Attack": 143,
      "Defense": 143,
      "Sp. Attack": 80,
      "Sp. Defense": 65,
      "Speed": 34
    }
  }]
''' pokemon ),


## We perform unnest and json_extract_array to make sure every data has its owm row.
  base AS (
  SELECT
    JSON_VALUE(pokemon_data,"$.id") AS id,
    JSON_VALUE(pokemon_data,"$.name.english") AS english,
      JSON_VALUE(pokemon_data,"$.name.japanese") AS japanese,
      JSON_VALUE(pokemon_data,"$.name.chinese") AS chinese,
      JSON_VALUE(pokemon_data,"$.name.french") AS french,
    JSON_EXTRACT(pokemon_data,"$.type") AS type,
    JSON_VALUE(pokemon_data,"$.base.HP") AS HP,
    JSON_VALUE(pokemon_data,"$.base.Attack") AS Attack,
    JSON_VALUE(pokemon_data,"$.base.Defense") AS Defense,
    JSON_VALUE(pokemon_data,"$.base.Speed") AS Speed,
    JSON_VALUE(pokemon_data,'$.base."Sp. Attack"') AS Sp_Attack,
    JSON_VALUE(pokemon_data,'$.base."Sp. Defense"') AS SP_Defense,
  FROM
    json_data
  LEFT JOIN
    UNNEST(JSON_EXTRACT_ARRAY(pokemon)) AS pokemon_data)



## We extract type then we  group english,japanese,chinese,french into name. We will  group HP,Attack,Defense,SP_attack,SP_defense,Speed into Base as well
SELECT
id,
struct(english,japanese,chinese,french) as name,
struct(HP,Attack,Defense,SP_Attack,Sp_Defense,Speed) as Base,
REPLACE(type,'"',"") as type 
FROM
  base
LEFT JOIN
  UNNEST(JSON_EXTRACT_ARRAY(type)) AS type

  
