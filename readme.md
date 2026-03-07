<https://vuaco.app/api/tournaments>
-> response: {key: key}[] -> _key_ = for each response get "key"
-> example:

```json
[
  {
    "key": "ngu-duong-boi",
  },
  {
    "key": "giai-vo-dich-the-gioi",
    
  },
]
```

for each _key_ do:
step 1: get editions
<https://vuaco.app/api/tournaments/{_key_}>
-> response = {editions: []} -> _year_ = for each edition in editions get "year"
-> example:

```json
{
  "key": "giai-vo-dich-the-gioi",
  "editions": [
    {
      "year": 2025,
      "slug": "giai-vo-dich-the-gioi-2025",
      "game_count": 294,
      "event_count": 1,
      "edition_number": 19,
      "champion": "Lại Lý Huynh",
      "champion_country": "Việt Nam",
      "champion_women": "Đường Đan",
      "champion_women_country": "Trung Quốc",
      "city": "Thượng Hải",
      "country": "Trung Quốc",
      "venue": "",
      "format": "Thụy Sĩ, 9 vòng (8+1 vòng quyết)",
      "date": "22-27/09/2025"
    }
  ]
}
```

for each _year_ do:
step 1: get games
<https://vuaco.app/api/tournaments/{_key_}/{_year_}>
-> response = {game:[], meta:{}} -> _game_id_ = for each game in game get "game_id"
-> example:
```json
{
  "games": [
    {
      "slug": "cat-chan-y-vs-truong-hoa",
      "red": "cat-chan-y",
      "red_vi": "Cát Chấn Y",
      "red_cn": "葛振衣",
      "red_en": "Ge Zhenyi",
      "black": "truong-hoa",
      "black_vi": "Trương Hoa",
      "black_cn": "张华",
      "black_en": "Zhang Hua",
      "result_key": "red_win",
      "event": "Giải vô địch thế giới lần thứ 19 (2025)",
      "total_moves": 95,
      "stage": "Nam",
      "group": "Vòng 1"
    },
    ]
    }

    ```
    for each _game_id_ do:
      step 1: get challenge and difficulty
      curl https://vuaco.app/api/analysis-challenge
      -> response = {challenge: "_challenge_", difficulty: "_difficulty_"}

      step 2: solve challenge
      solver_cli --challenge "_challenge_" --difficulty "_difficulty_"
      -> _solution_

      step 3: submit solution

      curl -X POST https://vuaco.app/api/analysis \
        -H "Content-Type: application/json" \
        -d '{"game_id":"_game_id_","challenge":"_challenge_","nonce":"_solution_"}' -o response.json
    end for:

end for:
end for:
