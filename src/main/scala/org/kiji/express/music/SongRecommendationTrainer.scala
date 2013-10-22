/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.music

import com.twitter.scalding.{
    GroupBuilder,
    Source
}

import org.kiji.express.{
    AvroRecord,
    EntityId,
    KijiSlice
}
import org.kiji.express.modeling.Trainer

class SongRecommendationTrainer extends Trainer {

  class TrainJob(
      inputs: Map[String, Source],
      outputs: Map[String, Source]) extends TrainerJob {

    /**
     * Transforms a slice of song ids into a collection of tuples `(s1,
     * s2)` signifying that `s2` appeared after `s1` in the slice, chronologically.
     *
     * @param slice of song ids representing a user's play history.
     * @return a list of song bigrams.
     */
    def bigrams(slice: KijiSlice[String]): List[(String, String)] = {
      slice.orderChronologically().cells.sliding(2)
      .map { window => window.iterator }
      .map { itr => (itr.next().datum, itr.next().datum) }
      .toList
    }

    /**
     * Transforms a group of tuples into a group containing a list of song count records,
     * sorted by count.
     *
     * @param nextSongs is the group of tuples containing song count records.
     * @return a group containing a list of song count records, sorted by count.
     */
    def sortNextSongs(nextSongs: GroupBuilder): GroupBuilder = {
      nextSongs.sortBy('count).reverse.toList[AvroRecord]('song_count -> 'top_songs)
    }

    inputs("user-song-plays")
        .flatMap('playlist -> ('first_song, 'song_id)) { bigrams }
        .groupBy(('first_song, 'song_id)) { _.size('count) }
        .packAvro(('song_id, 'count) -> 'song_count)
        .groupBy('first_song) { sortNextSongs }
        .packAvro('top_songs -> 'top_next_songs)
        .map('first_song -> 'entityId) { firstSong: String => EntityId(firstSong) }
        .write(outputs("top-next-songs"))

  }

  def train(inputs: Map[String, Source], outputs: Map[String, Source]) = {
    new TrainJob(inputs, outputs).run
  }
}

