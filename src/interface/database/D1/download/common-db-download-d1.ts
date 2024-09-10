/*!
 * MIT License
 *
 * Copyright (c) 2024 デジタル庁
 * Copyright (c) 2024 NEKOYASAN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
import { DataField } from "@config/data-field";
import { DbTableName } from "@config/db-table-name";
import { TableKeyProvider } from "@domain/services/table-key-provider";
import { ICommonDbDownload } from "@interface/database/common-db";
import { Sqlite3Wrapper } from "@interface/database/sqlite3/better-sqlite3-wrap";
import { Statement } from "better-sqlite3";
import {D1Database} from "@cloudflare/workers-types";
export class CommonDbDownloadD1
  implements ICommonDbDownload {
  private readonly d1Client: D1Database;

  constructor(d1Client: D1Database) {
    this.d1Client = d1Client;
  }

  async closeDb(): Promise<void> {
    // D1の場合はClose処理は不要
  }

  // Prefテーブルにデータを挿入する
  async prefCsvRows(rows: Record<string, string | number>[]) {
    const preparedStatement = this.d1Client.prepare(`
      INSERT INTO ${DbTableName.PREF} (
        pref_key,
        ${DataField.LG_CODE.dbColumn},
        ${DataField.PREF.dbColumn}
      ) VALUES (
        ?1,
        ?2,
        ?3
      ) ON CONFLICT (pref_key) DO UPDATE SET
        ${DataField.LG_CODE.dbColumn} = ?2,
        ${DataField.PREF.dbColumn} = ?3
      WHERE 
        pref_key = ?1
    `);


    const stmts = rows.map((row) => {
      row.pref_key = TableKeyProvider.getPrefKey({
        lg_code: row[DataField.LG_CODE.dbColumn] as string,
      });
      return preparedStatement.bind(row.pref_key, row.lg_code, row.pref);
    });

    await this.d1Client.batch(stmts);
  }

  // rep_lat, rep_lon を Prefテーブルに挿入/更新する
  async prefPosCsvRows(rows: Record<string, string | number>[]): Promise<void> {
    const preparedStatement = this.d1Client.prepare(`
      INSERT INTO ${DbTableName.PREF} (
        pref_key,
        ${DataField.REP_LAT.dbColumn},
        ${DataField.REP_LON.dbColumn}
      ) VALUES (
        ?1,
        ?2,
        ?3
      ) ON CONFLICT (pref_key) DO UPDATE SET
        ${DataField.REP_LAT.dbColumn} = ?2,
        ${DataField.REP_LON.dbColumn} = ?3
      WHERE 
        pref_key = ?1 AND (
          ${DataField.REP_LAT.dbColumn} != ?2 OR 
          ${DataField.REP_LON.dbColumn} != ?3 OR 
          ${DataField.REP_LAT.dbColumn} IS NULL OR
          ${DataField.REP_LON.dbColumn} IS NULL
        )
    `);
    const stmts = rows.map((row) => {
      row.pref_key = TableKeyProvider.getPrefKey({
        lg_code: row[DataField.LG_CODE.dbColumn] as string,
      });
      return preparedStatement.bind(row.pref_key, row.rep_lat, row.rep_lon);
    });

    await this.d1Client.batch(stmts);
  }

  // Cityテーブルにデータを挿入する
  async cityCsvRows(rows: Record<string, string | number>[]) {
    const preparedStatement = this.d1Client.prepare(`
      INSERT INTO ${DbTableName.CITY} (
        city_key,
        pref_key,
        ${DataField.LG_CODE.dbColumn},
        ${DataField.COUNTY.dbColumn},
        ${DataField.CITY.dbColumn},
        ${DataField.WARD.dbColumn}
      ) VALUES (
        ?1,
        ?2,
        ?3,
        ?4,
        ?5,
        ?6
      ) ON CONFLICT (city_key) DO UPDATE SET
        ${DataField.LG_CODE.dbColumn} = ?3,
        ${DataField.COUNTY.dbColumn} = ?4,
        ${DataField.CITY.dbColumn} = ?5,
        ${DataField.WARD.dbColumn} = ?6
      WHERE 
        city_key = ?1
    `);

    const prefKey = TableKeyProvider.getPrefKey({
      lg_code: rows[0][DataField.LG_CODE.dbColumn].toString(),
    });
    const stmts = rows.map((row) => {
      row.pref_key = prefKey;
      row.city_key = TableKeyProvider.getCityKey({
        lg_code: row[DataField.LG_CODE.dbColumn] as string,
      });
      return preparedStatement.bind(row.city_key, row.pref_key, row.lg_code, row.county, row.city, row.ward);
    })

    await this.d1Client.batch(stmts);
  }

  // rep_lat, rep_lon を Cityテーブルに挿入/更新する
  async cityPosCsvRows(rows: Record<string, string | number>[]): Promise<void> {
    const preparedStatement = this.d1Client.prepare(`
      INSERT INTO ${DbTableName.CITY} (
        city_key,
        pref_key,
        ${DataField.REP_LAT.dbColumn},
        ${DataField.REP_LON.dbColumn}
      ) VALUES (
        ?1,
        ?2,
        ?3,
        ?4
      ) ON CONFLICT (city_key) DO UPDATE SET
        ${DataField.REP_LAT.dbColumn} = ?3,
        ${DataField.REP_LON.dbColumn} = ?4
      WHERE 
        city_key = ?1 AND (
          ${DataField.REP_LAT.dbColumn} != ?3 OR 
          ${DataField.REP_LON.dbColumn} != ?4 OR 
          ${DataField.REP_LAT.dbColumn} IS NULL OR
          ${DataField.REP_LON.dbColumn} IS NULL
        )
    `);

    const prefKey = TableKeyProvider.getPrefKey({
      lg_code: rows[0][DataField.LG_CODE.dbColumn].toString(),
    });

    const stmts = rows.map((row) => {
      row.pref_key = prefKey;
      row.city_key = TableKeyProvider.getCityKey({
        lg_code: row[DataField.LG_CODE.dbColumn] as string,
      });
      return preparedStatement.bind(row.city_key, row.pref_key, row.rep_lat, row.rep_lon);
    })

    await this.d1Client.batch(stmts);
  }

  // Townテーブルにデータを挿入する
  async townCsvRows(rows: Record<string, string | number>[]) {
    const preparedStatement = this.d1Client.prepare(`
      INSERT INTO ${DbTableName.TOWN} (
        town_key,
        city_key,
        ${DataField.MACHIAZA_ID.dbColumn},
        ${DataField.OAZA_CHO.dbColumn},
        ${DataField.CHOME.dbColumn},
        ${DataField.KOAZA.dbColumn},
        ${DataField.RSDT_ADDR_FLG.dbColumn}
      ) VALUES (
        ?1,
        ?2,
        ?3,
        ?4,
        ?5,
        ?6,
        ?7
      ) ON CONFLICT (town_key) DO UPDATE SET
        ${DataField.MACHIAZA_ID.dbColumn} = ?3,
        ${DataField.OAZA_CHO.dbColumn} = ?4,
        ${DataField.CHOME.dbColumn} = ?5,
        ${DataField.KOAZA.dbColumn} = ?6,
        ${DataField.RSDT_ADDR_FLG.dbColumn} = ?7
      WHERE 
        town_key = ?1
    `);
    const cityKey = TableKeyProvider.getCityKey({
      lg_code: rows[0][DataField.LG_CODE.dbColumn].toString(),
    });

    const stmts = rows.map((row) => {
      row.city_key = cityKey;
      row.town_key = TableKeyProvider.getTownKey({
        lg_code: row[DataField.LG_CODE.dbColumn] as string,
        machiaza_id: row[DataField.MACHIAZA_ID.dbColumn] as string,
      }) as number;
      return preparedStatement.bind(row.town_key, row.city_key, row.machiaza_id, row.oaza_cho, row.chome, row.koaza, row.rsdt_addr_flg);
    });

    await this.d1Client.batch(stmts);
  }

  // rep_lat, rep_lon を Townテーブルに挿入/更新する
  async townPosCsvRows(rows: Record<string, string | number>[]): Promise<void> {
    const preparedStatement = this.d1Client.prepare(`
      INSERT INTO ${DbTableName.TOWN} (
        town_key,
        city_key,
        ${DataField.REP_LAT.dbColumn},
        ${DataField.REP_LON.dbColumn}
      ) VALUES (
        ?1,
        ?2,
        ?3,
        ?4
      ) ON CONFLICT (town_key) DO UPDATE SET
        ${DataField.REP_LAT.dbColumn} = ?3,
        ${DataField.REP_LON.dbColumn} = ?4
      WHERE 
        town_key = ?1 AND (
          ${DataField.REP_LAT.dbColumn} != ?3 OR 
          ${DataField.REP_LON.dbColumn} != ?4 OR 
          ${DataField.REP_LAT.dbColumn} IS NULL OR
          ${DataField.REP_LON.dbColumn} IS NULL
        )
    `);
    const cityKey = TableKeyProvider.getCityKey({
      lg_code: rows[0][DataField.LG_CODE.dbColumn].toString(),
    });

    const stmts = rows.map((row) => {
      row.city_key = cityKey;
      row.town_key = TableKeyProvider.getTownKey({
        lg_code: row[DataField.LG_CODE.dbColumn] as string,
        machiaza_id: row[DataField.MACHIAZA_ID.dbColumn] as string,
      }) as number;
      return preparedStatement.bind(row.town_key, row.city_key, row.rep_lat, row.rep_lon);
    });

    await this.d1Client.batch(stmts);
  }
}