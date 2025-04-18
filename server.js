const http = require("http");
const axios = require("axios");
const pool = require("./db");
const { PORT } = require("./config");
const puppeteer = require("puppeteer");

async function processSources(index = 0, sources = []) {
  if (index >= sources.length) {
    console.log("No more data to fetch");
    return;
  }

  const current = sources[index];

  try {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    await page.goto(
      `https://www.sofascore.com/api/v1/team/${current.club_identifier}/standings/seasons`,
      {
        waitUntil: "networkidle2",
      }
    );

    const body = await page.evaluate(() => document.body.innerText);

    //add categories in table sofascore_categories
    const categories = new Map();

    const data = await JSON.parse(body).tournamentSeasons;
    const now = new Date();

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
      console.log(`Category saved/updated: ${category.name}`);
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
      console.log(`Tournament saved/updated: ${name}`);
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
      console.log(`Season saved/updated: ${name}`);
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
          [tournament.id, seasonId, now, now]
        );
        console.log(
          `Tournament-Season entry saved/updated: Tournament ID = ${tournament.id}, Season ID = ${seasonId}`
        );
      }
    }

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
        const url = `https://www.sofascore.com/api/v1/unique-tournament/173/season/63807/statistics?limit=10&offset=20`;
        //TODO: replace url like above https://www.sofascore.com/api/v1/unique-tournament/173/season/63807/statistics?limit=10&offset=20

        try {
          const data = await page.evaluate(async (apiUrl) => {
            const res = await fetch(apiUrl);
            return await res.json();
          }, url);

          console.log(
            `Tournament: ${tournamentId}, Season: ${seasonId}, Offset: ${offset}`
          );
          // console.log(
          //   data?.results.map((item) => item.player.id),
          //   "hellos"
          // ); // Insert to DB here

          fetchPlayerData(962878);

          hasMore = false;
          //TODO: hasMore = data?.statistics?.length === limit;
          offset += limit;
        } catch (err) {
          console.error(
            `Puppeteer error for tournament ${tournamentId}, season ${seasonId}:`,
            err.message
          );
          hasMore = false;
        }
      }
    }

    //for sofascore_player_position

    async function savePlayerPositions(playerId, positions) {
      for (const position of positions) {
        // 1. Check if position exists
        const [positionRows] = await db.execute(
          "SELECT id FROM sofascore_position WHERE name = ? LIMIT 1",
          [position]
        );

        let positionId;

        if (positionRows.length === 0) {
          // Insert if not exists
          const [insertResult] = await db.execute(
            "INSERT INTO sofascore_position (name, created_at, updated_at) VALUES (?, NOW(), NOW())",
            [position]
          );
          positionId = insertResult.insertId;
          console.log(`✅ Inserted new position: ${position}`);
        } else {
          positionId = positionRows[0].id;
          console.log(`⚠️ Position already exists: ${position}`);
        }

        // 2. Insert into sofascore_player_position if not already there
        const [existingPlayerPosition] = await db.execute(
          "SELECT * FROM sofascore_player_position WHERE player_id = ? AND position_id = ? LIMIT 1",
          [playerId, positionId]
        );

        if (existingPlayerPosition.length === 0) {
          await db.execute(
            "INSERT INTO sofascore_player_position (player_id, position_id, created_at, updated_at) VALUES (?, ?, NOW(), NOW())",
            [playerId, positionId]
          );
          console.log(`✅ Added player ${playerId} to position ${position}`);
        } else {
          console.log(
            `⚠️ Player ${playerId} already linked to position ${position}`
          );
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
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

        const values = [
          playerId,
          data.team.id,
          data.uniqueTournament.id,
          data.year,
          data.statistics.accurateCrosses,
          data.statistics.accurateCrossesPercentage,
          data.statistics.accurateLongBalls,
          data.statistics.accurateLongBallsPercentage,
          data.statistics.accuratePasses,
          data.statistics.accuratePassesPercentage,
          data.statistics.aerialDuelsWon,
          data.statistics.assists,
          data.statistics.bigChancesCreated,
          data.statistics.bigChancesMissed,
          data.statistics.blockedShots,
          data.statistics.cleanSheet,
          data.statistics.dribbledPast,
          data.statistics.errorLeadToGoal,
          data.statistics.expectedAssists,
          data.statistics.expectedGoals,
          data.statistics.goals,
          data.statistics.goalsAssistsSum,
          data.statistics.goalsConceded,
          data.statistics.interceptions,
          data.statistics.keyPasses,
          data.statistics.minutesPlayed,
          data.statistics.passToAssist,
          data.statistics.rating,
          data.statistics.redCards,
          data.statistics.saves,
          data.statistics.shotsOnTarget,
          data.statistics.successfulDribbles,
          data.statistics.tackles,
          data.statistics.totalShots,
          data.statistics.yellowCards,
          data.statistics.totalRating,
          data.statistics.countRating,
          data.statistics.totalLongBalls,
          data.statistics.totalCross,
          data.statistics.totalPasses,
          data.statistics.shotsFromInsideTheBox,
          data.statistics.appearances,
          new Date(),
          new Date(),
        ];

        await pool.promise().execute(query, values);
        console.log("✅ Player statistics inserted successfully!");
      } catch (err) {
        console.error("❌ Error inserting player statistics:", err.message);
      }
    }

    async function savePlayerDetails(player) {
      const query = `
    INSERT INTO sofascore_player (
      id, name, first_name, slug, short_name, team_id, tournament_id, unique_tournament_id, position, jersey_number,
      height, preferred_foot, user_count, deceased, gender, country_alpha2, country_alpha3, country_name,
      country_slug, shirt_number, date_of_birth_timestamp, contract_until_timestamp, proposed_market_value_raw,
      proposed_market_value_raw_currency, created_at, updated_at
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?), ?, ?, NOW(), NOW()
    );
  `;

      const values = [
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
        player.dateOfBirthTimestamp || null,
        player.contractUntilTimestamp || null,
        player.proposedMarketValueRaw?.value || null,
        player.proposedMarketValueRaw?.currency || null,
      ];

      try {
        const [result] = await pool.execute(query, values);
        console.log("Insert successful:", result.insertId);
        return result;
      } catch (error) {
        console.error("Insert failed:", error.message);
        throw error;
      }
    }

    async function fetchPlayerData(playerId) {
      try {
        // 1) Player Details
        const playerDetails = await page.evaluate(async (id) => {
          const res = await fetch(
            `https://www.sofascore.com/api/v1/player/${id}`
          );
          return await res.json();
        }, playerId);
        savePlayerDetails(playerDetails?.player);
        // TODO: Save playerDetails to sofascore_player table

        // 2) Player Characteristics & Position
        const playerCharacteristics = await page.evaluate(async (id) => {
          const res = await fetch(
            `https://www.sofascore.com/api/v1/player/${id}/characteristics`
          );
          return await res.json();
        }, playerId);

        // playerCharacteristics.positions.map((item) => {
        //   console.log(item);
        // });
        savePlayerPositions(playerId, playerCharacteristics.positions);

        // console.log(
        //   `✅ Player ${playerId} characteristics:`,
        //   playerCharacteristics
        // );

        // TODO:
        // - Check if position exists in sofascore_position
        // - If not, insert it
        // - Then add to sofascore_player_position table

        // 3) Player Statistics
        const playerStatistics = await page.evaluate(async (id) => {
          const res = await fetch(
            `https://www.sofascore.com/api/v1/player/${id}/statistics`
          );
          return await res.json();
        }, playerId);
        console.log(`✅ Player ${playerId} statistics:`, playerStatistics);
        playerStatistics.seasons?.map((item) => {
          savePlayerStatistics(playerId, item);
        });
        // TODO: Save playerStatistics to sofascore_player_statistics
      } catch (err) {
        console.error(
          `❌ Error fetching data for player ${playerId}:`,
          err.message
        );
      }
    }

    const tournamentSeasons = await getTournamentSeasons();

    for (const {
      sofascore_tournament_id,
      sofascore_season_id,
    } of tournamentSeasons) {
      await fetchPaginatedStatistics(
        sofascore_tournament_id,
        sofascore_season_id
      );
    }

    await browser.close();

    // await processSources(index + 1, sources);
  } catch (error) {
    console.error(`❌ Error for source ID ${current.id}:`, error.message);
    // await processSources(index + 1, sources); // Continue even if one fails
  }
}

const server = http.createServer(async (req, res) => {
  if (req.url === "/process-data" && req.method === "GET") {
    try {
      const [rows] = await pool.query(
        "SELECT * FROM sofascore_club_scrap ORDER BY id ASC"
      );

      if (rows.length === 0) {
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end("No source data to process");
        return;
      }

      await processSources(0, rows);

      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("Processing complete");
    } catch (err) {
      console.error(err);
      res.writeHead(500, { "Content-Type": "text/plain" });
      res.end("Server Error");
    }
  } else {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not Found");
  }
});

server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
