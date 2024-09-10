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
import { IRsdtBlkDbDownload } from "@interface/database/common-db";
import { Sqlite3Wrapper } from "@interface/database/sqlite3/better-sqlite3-wrap";
import { Statement } from "better-sqlite3";
import {D1Database} from "@cloudflare/workers-types";

export class RsdtBlkDbDownloadD1 implements IRsdtBlkDbDownload {
  private readonly d1Client: D1Database;

  constructor(d1Client: D1Database) {
    this.d1Client = d1Client;
  }

  async closeDb(): Promise<void> {
    // D1の場合はClose処理は不要
  }

  // rep_lat, rep_lon を rsdt_blkテーブルに挿入/更新する
  async rsdtBlkPosCsvRows(rows: Record<string, string | number>[]): Promise<void> {
    const preparedStatement = this.d1Client.prepare(`
      INSERT INTO ${DbTableName.RSDT_BLK} (
        rsdtblk_key,
        town_key,
        ${DataField.REP_LAT.dbColumn},
        ${DataField.REP_LON.dbColumn}
      ) VALUES (
        ?1,
        ?2,
        ?3,
        ?4
      ) ON CONFLICT (rsdtblk_key) DO UPDATE SET
        ${DataField.REP_LAT.dbColumn} = ?3,
        ${DataField.REP_LON.dbColumn} = ?4
      WHERE 
        rsdtblk_key = ?1 AND (
          ${DataField.REP_LAT.dbColumn} != ?3 OR 
          ${DataField.REP_LON.dbColumn} != ?4 OR 
          ${DataField.REP_LAT.dbColumn} IS NULL OR
          ${DataField.REP_LON.dbColumn} IS NULL
        )
    `);

    const lg_code = rows[0][DataField.LG_CODE.dbColumn].toString();

    const stmts = rows.map((row) => {
      row.town_key = TableKeyProvider.getTownKey({
        lg_code,
        machiaza_id: row[DataField.MACHIAZA_ID.dbColumn].toString(),
      })!;

      row.rsdtblk_key = TableKeyProvider.getRsdtBlkKey({
        lg_code: row[DataField.LG_CODE.dbColumn] as string,
        machiaza_id: row[DataField.MACHIAZA_ID.dbColumn] as string,
        blk_id: row[DataField.BLK_ID.dbColumn] as string,
      })
      return preparedStatement.bind(row.rsdtblk_key, row.town_key, row.rep_lat, row.rep_lon);
    });

    await this.d1Client.batch(stmts);
  }

  // テーブルにcsvのデータを溜め込む
  async rsdtBlkCsvRows(rows: Record<string, string | number>[]) {
    const preparedStatement = this.d1Client.prepare(`
      INSERT INTO ${DbTableName.RSDT_BLK} (
        rsdtblk_key,
        town_key,
        ${DataField.BLK_ID.dbColumn},
        ${DataField.BLK_NUM.dbColumn}
      ) VALUES (
        ?1,
        ?2,
        ?3,
        ?4
      ) ON CONFLICT (rsdtblk_key) DO UPDATE SET
        ${DataField.BLK_ID.dbColumn} = ?3,
        ${DataField.BLK_NUM.dbColumn} = ?4
      WHERE 
        rsdtblk_key = ?1
    `);


    const lg_code = rows[0][DataField.LG_CODE.dbColumn].toString();

    const stmts = rows.map((row) => {
      row.town_key = TableKeyProvider.getTownKey({
        lg_code,
        machiaza_id: row[DataField.MACHIAZA_ID.dbColumn].toString(),
      })!;

      row.rsdtblk_key = TableKeyProvider.getRsdtBlkKey({
        lg_code: row[DataField.LG_CODE.dbColumn] as string,
        machiaza_id: row[DataField.MACHIAZA_ID.dbColumn] as string,
        blk_id: row[DataField.BLK_ID.dbColumn] as string,
      })
      return preparedStatement.bind(row.rsdtblk_key, row.town_key, row.blk_id, row.blk_num);
    });

    await this.d1Client.batch(stmts);
  }
}