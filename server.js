const http = require("http");
const pool = require("./db");
const { PORT } = require("./config");
const puppeteer = require("puppeteer");
const { default: axios } = require("axios");

async function processSources(index = 0, sources = []) {
  const API_KEY = "2ac18ab6d1394c81be65cdd16b406082";

  if (index >= sources.length) {
    console.log("No more data to fetch");
    return;
  }

  const current = sources[index];
  const now = new Date();

  //function to decode the data
  function decodeAndParseJSON(base64Body) {
    try {
      const decodedString = Buffer.from(base64Body, "base64").toString("utf-8");

      // Check if it's an HTML error page
      if (decodedString.trim().startsWith("<")) {
        console.warn("⚠️ Received HTML instead of JSON.");
        console.log(decodedString); // Optional: log the HTML for debugging
        return null;
      }

      // Parse and return the JSON
      return JSON.parse(decodedString);
    } catch (error) {
      console.error("❌ Error decoding or parsing JSON:", error);
      return null;
    }
  }

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  const seasons = await axios.post(
    "https://api.zyte.com/v1/extract",
    {
      url: `https://www.sofascore.com/api/v1/team/${current.club_identifier}/standings/seasons`,
      httpResponseBody: true,
      httpRequestMethod: "GET",
    },
    {
      auth: {
        username: API_KEY,
      },
    }
  );
  const allSeasonsData = await decodeAndParseJSON(
    seasons.data.httpResponseBody
  );
  const data = (await allSeasonsData?.tournamentSeasons) || [];
  if (seasons.data.statusCode === 200) {
    if (data.length > 0) {
      //add categories in table sofascore_categories
      const categories = new Map();

      await data.forEach((tournamentSeason) => {
        const cat = tournamentSeason.tournament.category;
        if (!categories.has(cat.id)) {
          categories.set(cat.id, {
            id: cat.id,
            name: cat.name,
            slug: cat.slug,
          });
        }
      });

      for (const category of categories.values()) {
        await pool.query(
          `INSERT INTO sofascore_categories (id, name, slug,created_at, updated_at)
         VALUES (?, ?, ?,?,?)
         ON DUPLICATE KEY UPDATE 
     name = VALUES(name), 
     slug = VALUES(slug), 
     updated_at = VALUES(updated_at)`,
          [category.id, category.name, category.slug, now, now]
        );
      }

      //add tournaments in table sofascore_tournaments
      const tournaments = Object.values(
        await data.reduce((acc, ts) => {
          const t = ts.tournament;
          if (!acc[t.id]) {
            acc[t.id] = {
              id: t.id,
              name: t.name,
              slug: t.slug,
              category_id: t.category.id,
              unique_tournament_id: t.uniqueTournament.id,
            };
          }
          return acc;
        }, {})
      );

      for (const {
        id,
        name,
        slug,
        category_id,
        unique_tournament_id,
      } of tournaments) {
        await pool.query(
          `INSERT INTO sofascore_tournaments (id, name, slug, category_id, unique_tournament_id, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE 
       name = VALUES(name), 
       slug = VALUES(slug), 
       category_id = VALUES(category_id),
       unique_tournament_id = VALUES(unique_tournament_id),
       updated_at = VALUES(updated_at)`,
          [id, name, slug, category_id, unique_tournament_id, now, now]
        );
      }
      //add seasons in table sofascore_seasons
      const seasonsMap = new Map();

      await data.forEach(({ seasons }) => {
        seasons.forEach(({ id, name, year }) => {
          if (!seasonsMap.has(id)) {
            seasonsMap.set(id, { id, name, year });
          }
        });
      });

      for (const { id, name, year } of seasonsMap.values()) {
        await pool.query(
          `INSERT INTO sofascore_seasons (id, name, year, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE 
       name = VALUES(name), 
       year = VALUES(year), 
       updated_at = VALUES(updated_at)`,
          [id, name, year, now, now]
        );
      }

      //add seasons in table sofascore_season_tournament_season
      for (const { tournament, seasons } of data) {
        for (const { id: seasonId } of seasons) {
          await pool.query(
            `INSERT INTO sofascore_tournament_season (sofascore_tournament_id, sofascore_season_id, created_at, updated_at)
       VALUES (?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE 
         sofascore_tournament_id = VALUES(sofascore_tournament_id),
         sofascore_season_id = VALUES(sofascore_season_id),
         updated_at = VALUES(updated_at)`,
            [tournament.uniqueTournament.id, seasonId, now, now]
          );
        }
      }
    }
    const response = await axios.post(
      "https://api.zyte.com/v1/extract",
      {
        url: `https://www.sofascore.com/api/v1/team/${current.club_identifier}/performance`,
        httpResponseBody: true,
        httpRequestMethod: "GET",
      },
      {
        auth: {
          username: API_KEY,
        },
      }
    );

    if (response.data.statusCode === 200) {
      const fixturePageDetails = await decodeAndParseJSON(
        response.data.httpResponseBody
      );

      await fixturePageDetails?.events?.map((item) => {
        saveFixtures(item);
      });
    } else {
      await processSources(index, sources);
    }
    //TODO: +1 logic here
  } else {
    await processSources(index, sources);
  }

  //to add fixtures

  const saveFixtures = async (fixture) => {
    const normalizeValues = (arr) =>
      arr.map((v) => (v === undefined ? null : v));

    try {
      // Check if the fixture already exists in the database
      const [existingFixtureRows] = await pool.query(
        "SELECT id FROM sofascore_fixture WHERE id = ?",
        [fixture.id]
      );

      if (existingFixtureRows.length > 0) {
        // If fixture exists, perform an UPDATE
        const updateQuery = `
        UPDATE sofascore_fixture
        SET 
          slug = ?, tournament_id = ?, unique_tournament_id = ?, season_id = ?,
          round_info = ?, status_type = ?, winner_code = ?, home_team = ?, away_team = ?,
          home_team_score_current = ?, home_team_score_display = ?, home_team_score_period1 = ?,
          home_team_score_period2 = ?, home_team_score_normal_time = ?, away_team_score_current = ?,
          away_team_score_display = ?, away_team_score_period1 = ?, away_team_score_period2 = ?,
          away_team_score_normal_time = ?, current_period_start_timestamp = ?, start_timestamp = ?,
          updated_at = NOW()
        WHERE id = ?
      `;

        const updateValues = [
          fixture.slug,
          fixture.tournament?.id,
          fixture.tournament?.uniqueTournament?.id,
          fixture.season?.id,
          fixture.roundInfo?.round,
          fixture.status?.type,
          fixture.winnerCode,
          fixture.homeTeam?.id,
          fixture.awayTeam?.id,
          fixture.homeScore?.current,
          fixture.homeScore?.display,
          fixture.homeScore?.period1,
          fixture.homeScore?.period2,
          fixture.homeScore?.normalTime,
          fixture.awayScore?.current,
          fixture.awayScore?.display,
          fixture.awayScore?.period1,
          fixture.awayScore?.period2,
          fixture.awayScore?.normalTime,
          fixture.startTimestamp,
          fixture.startTimestamp,
          fixture.id,
        ];

        await pool.execute(updateQuery, normalizeValues(updateValues));
      } else {
        // If fixture does not exist, perform an INSERT
        const insertQuery = `
        INSERT INTO sofascore_fixture (
          id, slug, tournament_id, unique_tournament_id, season_id,
          round_info, status_type, winner_code, home_team, away_team,
          home_team_score_current, home_team_score_display,
          home_team_score_period1, home_team_score_period2,
          home_team_score_normal_time, away_team_score_current,
          away_team_score_display, away_team_score_period1,
          away_team_score_period2, away_team_score_normal_time,
          current_period_start_timestamp, start_timestamp,
          created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
      `;

        const insertValues = [
          fixture.id,
          fixture.slug,
          fixture.tournament?.id,
          fixture.tournament?.uniqueTournament?.id,
          fixture.season?.id,
          fixture.roundInfo?.round,
          fixture.status?.type,
          fixture.winnerCode,
          fixture.homeTeam?.id,
          fixture.awayTeam?.id,
          fixture.homeScore?.current,
          fixture.homeScore?.display,
          fixture.homeScore?.period1,
          fixture.homeScore?.period2,
          fixture.homeScore?.normalTime,
          fixture.awayScore?.current,
          fixture.awayScore?.display,
          fixture.awayScore?.period1,
          fixture.awayScore?.period2,
          fixture.awayScore?.normalTime,
          fixture.startTimestamp,
          fixture.startTimestamp,
        ];

        await pool.execute(insertQuery, normalizeValues(insertValues));
      }

      // Handle team entries (home and away) in sofascore_club_scrap
      if (fixture.homeTeam?.id) {
        const [homeTeamRows] = await pool.query(
          "SELECT id FROM sofascore_club_scrap WHERE club_identifier = ?",
          [fixture.homeTeam.id]
        );

        if (homeTeamRows.length === 0) {
          const insertHomeTeamQuery = `
          INSERT INTO sofascore_club_scrap (league_id, club_name, club_link, club_slug, club_identifier, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, NOW(), NOW())
        `;
          await pool.query(insertHomeTeamQuery, [
            fixture.tournament?.uniqueTournament?.id,
            fixture.homeTeam.name,
            `https://www.sofascore.com/team/football/${fixture.homeTeam.slug}/${fixture.homeTeam.id}`,
            fixture.homeTeam.slug,
            fixture.homeTeam.id,
          ]);
        }
      }

      if (fixture.awayTeam?.id) {
        const [awayTeamRows] = await pool.query(
          "SELECT id FROM sofascore_club_scrap WHERE club_identifier = ?",
          [fixture.awayTeam.id]
        );

        if (awayTeamRows.length === 0) {
          const insertAwayTeamQuery = `
          INSERT INTO sofascore_club_scrap (league_id, club_name, club_link, club_slug, club_identifier, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, NOW(), NOW())
        `;
          await pool.query(insertAwayTeamQuery, [
            fixture.tournament?.uniqueTournament?.id,
            fixture.awayTeam.name,
            `https://www.sofascore.com/team/football/${fixture.awayTeam.slug}/${fixture.awayTeam.id}`,
            fixture.awayTeam.slug,
            fixture.awayTeam.id,
          ]);
        }
      }
    } catch (error) {
      console.error("Insert or update failed:", error.message);
      throw error;
    }
  };

  await sleep(5000);

  //now get players from tournament and season
  async function getTournamentSeasons() {
    const [rows] = await pool.query(
      `SELECT sofascore_tournament_id, sofascore_season_id FROM sofascore_tournament_season`
    );
    return rows;
  }

  async function fetchPaginatedStatistics(tournamentId, seasonId) {
    const limit = 10;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const url = `https://www.sofascore.com/api/v1/unique-tournament/${tournamentId}/season/${seasonId}/statistics?limit=${limit}&offset=${offset}`;

      try {
        const response = await axios.post(
          "https://api.zyte.com/v1/extract",
          {
            url: url,
            httpResponseBody: true,
            httpRequestMethod: "GET",
          },
          {
            auth: {
              username: API_KEY,
            },
          }
        );
        const data = await decodeAndParseJSON(response.data.httpResponseBody);
        const delayBetweenRequests = 20000;

        //TODO: remove these below code after testing

        data?.results?.forEach((player, index) => {
          setTimeout(async () => {
            //TODO: uncomment below line
            await fetchPlayerData(player.player.id);
          }, delayBetweenRequests);
        });
        // if (offset === data?.pages * 10 - 10) {
        //   hasMore = false;
        // }
        //TODO: remove these hasMore it is
        hasMore = false;
        // offset += limit;
      } catch (err) {
        console.error(
          `Puppeteer error for tournament ${tournamentId}, season ${seasonId}:`,
          err.message
        );
      }
    }
  }

  //for sofascore_player_position

  async function savePlayerPositions(playerId, positions) {
    for (const position of positions) {
      // 1. Check if position exists
      const [positionRows] = await pool.execute(
        "SELECT id FROM sofascore_position WHERE name = ? LIMIT 1",
        [position]
      );

      let positionId;

      if (positionRows.length === 0) {
        // Insert if not exists
        const [insertResult] = await pool.execute(
          "INSERT INTO sofascore_position (name, created_at, updated_at) VALUES (?, NOW(), NOW())",
          [position]
        );
        positionId = insertResult.insertId;
      } else {
        positionId = positionRows[0].id;
      }

      // 2. Insert into sofascore_player_position if not already there
      const [existingPlayerPosition] = await pool.execute(
        "SELECT * FROM sofascore_player_position WHERE player_id = ? AND position_id = ? LIMIT 1",
        [playerId, positionId]
      );

      if (existingPlayerPosition.length === 0) {
        await pool.execute(
          "INSERT INTO sofascore_player_position (player_id, position_id, created_at, updated_at) VALUES (?, ?, NOW(), NOW())",
          [playerId, positionId]
        );
      } else {
      }
    }
  }

  //for sofascore_player_statistics
  async function savePlayerStatistics(playerId, data) {
    try {
      const query = `
      INSERT INTO sofascore_player_statistics (
        player_id, team_id, unique_tournament_id, year,
        accurate_crosses, accurate_crosses_percentage, accurate_long_balls, accurate_long_balls_percentage,
        accurate_passes, accurate_passes_percentage, aerial_duels_won, assists, big_chances_created,
        big_chances_missed, blocked_shots, clean_sheet, dribbled_past, error_lead_to_goal,
        expected_assists, expected_goals, goals, goals_assists_sum, goals_conceded,
        interceptions, key_passes, minutes_played, pass_to_assist, rating,
        red_cards, saves, shots_on_target, successful_dribbles, tackles,
        total_shots, yellow_cards, total_rating, count_rating, total_long_balls,
        total_cross, total_passes, shots_from_inside_the_box, appearances,
        created_at, updated_at
      ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      )
      ON DUPLICATE KEY UPDATE
        accurate_crosses = VALUES(accurate_crosses),
        accurate_crosses_percentage = VALUES(accurate_crosses_percentage),
        accurate_long_balls = VALUES(accurate_long_balls),
        accurate_long_balls_percentage = VALUES(accurate_long_balls_percentage),
        accurate_passes = VALUES(accurate_passes),
        accurate_passes_percentage = VALUES(accurate_passes_percentage),
        aerial_duels_won = VALUES(aerial_duels_won),
        assists = VALUES(assists),
        big_chances_created = VALUES(big_chances_created),
        big_chances_missed = VALUES(big_chances_missed),
        blocked_shots = VALUES(blocked_shots),
        clean_sheet = VALUES(clean_sheet),
        dribbled_past = VALUES(dribbled_past),
        error_lead_to_goal = VALUES(error_lead_to_goal),
        expected_assists = VALUES(expected_assists),
        expected_goals = VALUES(expected_goals),
        goals = VALUES(goals),
        goals_assists_sum = VALUES(goals_assists_sum),
        goals_conceded = VALUES(goals_conceded),
        interceptions = VALUES(interceptions),
        key_passes = VALUES(key_passes),
        minutes_played = VALUES(minutes_played),
        pass_to_assist = VALUES(pass_to_assist),
        rating = VALUES(rating),
        red_cards = VALUES(red_cards),
        saves = VALUES(saves),
        shots_on_target = VALUES(shots_on_target),
        successful_dribbles = VALUES(successful_dribbles),
        tackles = VALUES(tackles),
        total_shots = VALUES(total_shots),
        yellow_cards = VALUES(yellow_cards),
        total_rating = VALUES(total_rating),
        count_rating = VALUES(count_rating),
        total_long_balls = VALUES(total_long_balls),
        total_cross = VALUES(total_cross),
        total_passes = VALUES(total_passes),
        shots_from_inside_the_box = VALUES(shots_from_inside_the_box),
        appearances = VALUES(appearances),
        updated_at = NOW()
    `;

      const rawValues = [
        playerId,
        data.team?.id,
        data.uniqueTournament?.id,
        data.year,
        data.statistics?.accurateCrosses,
        data.statistics?.accurateCrossesPercentage,
        data.statistics?.accurateLongBalls,
        data.statistics?.accurateLongBallsPercentage,
        data.statistics?.accuratePasses,
        data.statistics?.accuratePassesPercentage,
        data.statistics?.aerialDuelsWon,
        data.statistics?.assists,
        data.statistics?.bigChancesCreated,
        data.statistics?.bigChancesMissed,
        data.statistics?.blockedShots,
        data.statistics?.cleanSheet,
        data.statistics?.dribbledPast,
        data.statistics?.errorLeadToGoal,
        data.statistics?.expectedAssists,
        data.statistics?.expectedGoals,
        data.statistics?.goals,
        data.statistics?.goalsAssistsSum,
        data.statistics?.goalsConceded,
        data.statistics?.interceptions,
        data.statistics?.keyPasses,
        data.statistics?.minutesPlayed,
        data.statistics?.passToAssist,
        data.statistics?.rating,
        data.statistics?.redCards,
        data.statistics?.saves,
        data.statistics?.shotsOnTarget,
        data.statistics?.successfulDribbles,
        data.statistics?.tackles,
        data.statistics?.totalShots,
        data.statistics?.yellowCards,
        data.statistics?.totalRating,
        data.statistics?.countRating,
        data.statistics?.totalLongBalls,
        data.statistics?.totalCross,
        data.statistics?.totalPasses,
        data.statistics?.shotsFromInsideTheBox,
        data.statistics?.appearances,
        new Date(),
        new Date(),
      ];

      const values = sanitizeValues(rawValues);

      await pool.execute(query, values);
    } catch (err) {
      console.error(
        "❌ Error inserting/updating player statistics:",
        err.message
      );
    }
  }

  async function savePlayerDetails(player) {
    if (player.id) {
      const query = `
    INSERT INTO sofascore_player (
      id, name, first_name, slug, short_name, team_id, tournament_id, unique_tournament_id, position, jersey_number,
      height, preferred_foot, user_count, deceased, gender, country_alpha2, country_alpha3, country_name,
      country_slug, shirt_number, date_of_birth_timestamp, contract_until_timestamp, proposed_market_value_raw,
      proposed_market_value_currency, created_at, updated_at
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?), ?, ?, NOW(), NOW()
    )
    ON DUPLICATE KEY UPDATE
      name = VALUES(name),
      first_name = VALUES(first_name),
      slug = VALUES(slug),
      short_name = VALUES(short_name),
      team_id = VALUES(team_id),
      tournament_id = VALUES(tournament_id),
      unique_tournament_id = VALUES(unique_tournament_id),
      position = VALUES(position),
      jersey_number = VALUES(jersey_number),
      height = VALUES(height),
      preferred_foot = VALUES(preferred_foot),
      user_count = VALUES(user_count),
      deceased = VALUES(deceased),
      gender = VALUES(gender),
      country_alpha2 = VALUES(country_alpha2),
      country_alpha3 = VALUES(country_alpha3),
      country_name = VALUES(country_name),
      country_slug = VALUES(country_slug),
      shirt_number = VALUES(shirt_number),
      date_of_birth_timestamp = FROM_UNIXTIME(?),
      contract_until_timestamp = FROM_UNIXTIME(?),
      proposed_market_value_raw = VALUES(proposed_market_value_raw),
      proposed_market_value_currency = VALUES(proposed_market_value_currency),
      updated_at = NOW();
  `;

      const rawValues = [
        player.id,
        player.name,
        player.firstName,
        player.slug,
        player.shortName,
        player.team?.id || null,
        player.team?.tournament?.id || null,
        player.team?.primaryUniqueTournament?.id || null,
        player.position || null,
        player.jerseyNumber || player.shirtNumber || null,
        player.height || null,
        player.preferredFoot || null,
        player.userCount || null,
        player.deceased || false,
        player.gender || null,
        player.country?.alpha2 || null,
        player.country?.alpha3 || null,
        player.country?.name || null,
        player.country?.slug || null,
        player.shirtNumber || null,
        player.dateOfBirthTimestamp || null, // FROM_UNIXTIME(?)
        player.contractUntilTimestamp || null, // FROM_UNIXTIME(?)
        player.proposedMarketValueRaw?.value || null,
        player.proposedMarketValueRaw?.currency || null,
        player.dateOfBirthTimestamp || null, // EXTRA for ON DUPLICATE KEY UPDATE
        player.contractUntilTimestamp || null, // EXTRA for ON DUPLICATE KEY UPDATE
      ];

      const values = sanitizeValues(rawValues);

      try {
        const [result] = await pool.execute(query, values);
        return result;
      } catch (error) {
        console.error("Insert failed:", error.message);
        throw error;
      }
    }
  }

  function sanitizeValues(values) {
    return values.map((v) => (v === undefined ? null : v));
  }

  async function fetchPlayerData(playerId) {
    // 1) Player Details
    const playerDetailResponse = await axios.post(
      "https://api.zyte.com/v1/extract",
      {
        url: `https://www.sofascore.com/api/v1/player/${playerId}`,
        httpResponseBody: true,
        httpRequestMethod: "GET",
      },
      {
        auth: {
          username: API_KEY,
        },
      }
    );

    const playerDetails = await decodeAndParseJSON(
      playerDetailResponse.data.httpResponseBody
    );
    await savePlayerDetails(playerDetails?.player);
    await sleep(5000);

    // 2) Player Characteristics & Position
    const playerCharacteristicsResponse = await axios.post(
      "https://api.zyte.com/v1/extract",
      {
        url: `https://www.sofascore.com/api/v1/player/${playerId}/characteristics`,
        httpResponseBody: true,
        httpRequestMethod: "GET",
      },
      {
        auth: {
          username: API_KEY,
        },
      }
    );

    if (playerCharacteristicsResponse.data.statusCode === 200) {
      const playerCharacteristics = await decodeAndParseJSON(
        playerCharacteristicsResponse.data.httpResponseBody
      );

      if (playerCharacteristicsResponse.data.statusCode === 200) {
        await savePlayerPositions(playerId, playerCharacteristics.positions);
      }
    } else {
      console.log(`${response.data.statusCode} error`);
    }

    await sleep(5000);
    // 3) Player Statistics
    const playerStatisticsResponse = await axios.post(
      "https://api.zyte.com/v1/extract",
      {
        url: `https://www.sofascore.com/api/v1/player/${playerId}/statistics`,
        httpResponseBody: true,
        httpRequestMethod: "GET",
      },
      {
        auth: {
          username: API_KEY,
        },
      }
    );
    if (playerStatisticsResponse.data.statusCode === 200) {
      const playerStatistics = await decodeAndParseJSON(
        playerStatisticsResponse.data.httpResponseBody
      );
      playerStatistics.seasons?.map(async (item) => {
        await savePlayerStatistics(playerId, item);
      });
    } else {
      console.log(`${response.data.statusCode} error`);
    }
  }

  const tournamentSeasons = await getTournamentSeasons();
  await sleep(5000);
  await fetchPaginatedStatistics(
    tournamentSeasons?.[0]?.sofascore_tournament_id,
    tournamentSeasons?.[0]?.sofascore_season_id
  );

  // for (const {
  //   sofascore_tournament_id,
  //   sofascore_season_id,
  // } of tournamentSeasons) {
  //   //TODO: uncomment below code
  //   // await fetchPaginatedStatistics(
  //   //   sofascore_tournament_id,
  //   //   sofascore_season_id
  //   // );
  // }
  //TODO: uncomment below code
  // await processSources(index + 1, sources);
}

const server = http.createServer(async (req, res) => {
  if (req.url === "/process-data" && req.method === "GET") {
    // Send immediate response
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end(
      JSON.stringify({
        statusCode: 200,
        message: "Processing started in background",
      })
    );

    try {
      const [rows] = await pool.query(
        "SELECT * FROM sofascore_club_scrap ORDER BY id ASC"
      );

      if (rows.length === 0) {
        console.log("No source data to process");
        return;
      }

      // Run background job
      setImmediate(async () => {
        try {
          await processSources(0, rows);
          console.log("✅ Background processing complete");
        } catch (err) {
          console.error("❌ Error in background job:", err.message);
        }
      });
    } catch (err) {
      console.error("❌ DB Fetch Error:", err.message);
    }
  } else {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not Found");
  }
});

server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
