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
    
  }
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
    }
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
      -> response if response.result is array and greater than 0 then export file {_key_}/{_year_}/{game_id}.json with content response.result
      -> response if response has job_id then do:
        example:

```json
{
    "job_id": "fd5ad3d1-e1e5-4a67-88cf-51ea5859e1a8",
    "status": "running",
    "result": null,
}
```

        step 4: check result
        curl https://vuaco.app/api/analysis/{job_id}
        -> response if response.result is array and greater than 0 then export file {_key_}/{_year_}/{game_id}.json with content response.result
      
    end for:

end for:
end for:


```js
// logic handle for step 3 and step 4
Sa = async D => {
        const {challenge: W, nonce: hn} = await Hx()
          , bn = {
            challenge: W,
            nonce: hn
        };
        D.gameId ? bn.game_id = D.gameId : (bn.moves = D.moves,
        D.fen && (bn.fen = D.fen));
        const An = await fetch("/api/analysis", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(bn)
        });
        if (!An.ok)
            throw new Error("Submit failed");
        const Bn = await An.json()
          , ut = Bn.game_meta
          , gt = Bn.raw_moves;
        if (Bn.status === "done")
            return {
                result: Bn.result,
                gameMeta: ut,
                rawMoves: gt
            };
        const Lt = Date.now()
          , Jn = 600 * 1e3
          , Fe = 60 * 1e3;
        let Wi = -1
          , Gn = Date.now();
        for (; ; ) {
            if (await new Promise(Dl => setTimeout(Dl, 2e3)),
            Date.now() - Lt > Jn)
                throw new Error("Analysis timed out (10 minutes)");
            const bi = await fetch(`/api/analysis/${Bn.job_id}`);
            if (!bi.ok)
                throw new Error("Polling failed");
            const Ye = await bi.json();
            if (Ye.status === "done")
                return {
                    result: Ye.result,
                    gameMeta: ut,
                    rawMoves: gt
                };
            if (Ye.status === "error")
                throw new Error(Ye.error || "Analysis failed");
            const kc = Ye.progress ?? 0;
            if (kc !== Wi)
                Wi = kc,
                Gn = Date.now();
            else if (Date.now() - Gn > Fe)
                throw new Error("Analysis stalled (no progress for 60s)")
        }
    }
```