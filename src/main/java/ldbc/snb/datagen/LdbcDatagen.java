/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.hadoop.generator.HadoopKnowsGenerator;
import ldbc.snb.datagen.hadoop.generator.HadoopPersonActivityGenerator;
import ldbc.snb.datagen.hadoop.generator.HadoopPersonGenerator;
import ldbc.snb.datagen.hadoop.miscjob.HadoopMergeFriendshipFiles;
import ldbc.snb.datagen.hadoop.serializer.HadoopPersonSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopPersonSortAndSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopStaticSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopUpdateStreamSorterAndSerializer;
import ldbc.snb.datagen.util.ConfigParser;
import ldbc.snb.datagen.vocabulary.SN;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class LdbcDatagen {
    private static boolean initialized = false;

    public static synchronized void initializeContext(Configuration conf) {
        if (!initialized) {
            DatagenParams.readConf(conf);
            Dictionaries.loadDictionaries(conf);
            SN.initialize();
            try {
                Person.personSimilarity = (Person.PersonSimilarity) Class
                        .forName(conf.get("ldbc.snb.datagen.generator.person.similarity")).newInstance();
            } catch (Exception e) {
                System.err.println("Error while loading person similarity class");
                System.err.println(e.getMessage());
            }
            initialized = true;
        }
    }

    private void printProgress(String message) {
        System.out.println("************************************************");
        System.out.println("* " + message + " *");
        System.out.println("************************************************");
    }

    public int runGenerateJob(Configuration conf) throws Exception {

        String hadoopPrefix = conf.get("ldbc.snb.datagen.serializer.hadoopDir");
        List<Float> percentages = new ArrayList<>();
        percentages.add(0.45f);
        percentages.add(0.45f);
        percentages.add(0.1f);

        printProgress("Starting: Person generation");
        long startPerson = System.currentTimeMillis();
        HadoopPersonGenerator personGenerator = new HadoopPersonGenerator(conf);
        personGenerator.run(hadoopPrefix + "/persons", "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter");
        long endPerson = System.currentTimeMillis();

        System.out.println("Person generation time: " + ((endPerson - startPerson) / 1000));
        return 0;
    }

    public static void prepareConfiguration(Configuration conf) throws Exception {

        conf.set("ldbc.snb.datagen.serializer.hadoopDir", conf
                .get("ldbc.snb.datagen.serializer.outputDir") + "/hadoop");
        conf.set("ldbc.snb.datagen.serializer.socialNetworkDir", conf
                .get("ldbc.snb.datagen.serializer.outputDir") + "/social_network");

        // Deleting existing files
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")), true);
        dfs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir")), true);
        FileUtils.deleteDirectory(new File(conf.get("ldbc.snb.datagen.serializer.outputDir")
                                                   + "/substitution_parameters"));

        ConfigParser.printConfig(conf);

    }

    public static void main(String[] args) throws Exception {

        try {
            Configuration conf = ConfigParser.initialize();
            ConfigParser.readConfig(conf, args[0]);
            ConfigParser.readConfig(conf, LdbcDatagen.class.getResourceAsStream("/params_default.ini"));

            LdbcDatagen.prepareConfiguration(conf);
            LdbcDatagen.initializeContext(conf);
            LdbcDatagen datagen = new LdbcDatagen();
            datagen.runGenerateJob(conf);
        } catch (Exception e) {
            System.err.println("Error during execution");
            System.err.println(e.getMessage());
            throw e;
        }
    }
}
