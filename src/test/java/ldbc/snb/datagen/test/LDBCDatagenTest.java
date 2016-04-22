package ldbc.snb.datagen.test;

import ldbc.snb.datagen.test.csv.*;
import org.codehaus.groovy.vmplugin.v5.JUnit4Utils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by aprat on 18/12/15.
 */
public class LDBCDatagenTest {

    final String dir = "./test_data/social_network";
    final String sdir = "./test_data/substitution_parameters";

    @BeforeClass
    public static void generateData() {
        ProcessBuilder pb = new ProcessBuilder("java", "-cp", "target/ldbc_snb_datagen.jar","org.apache.hadoop.util.RunJar","target/ldbc_snb_datagen.jar","./test_params.ini");
        pb.directory(new File("./"));
        File log = new File("test_log");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
        try {
            Process p = pb.start();
            p.waitFor();
        }catch(Exception e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void personTest() {
        testIdUniqueness(dir+"/person_0_0.csv", 0);
    }

    @Test
    public void postTest() {
        testIdUniqueness(dir+"/post_0_0.csv", 0);
    }

    @Test
    public void forumTest() {
        testIdUniqueness(dir+"/forum_0_0.csv", 0);
    }

    @Test
    public void commentTest() {
        testIdUniqueness(dir+"/comment_0_0.csv", 0);
    }

    @Test
    public void organisationTest() {
        testIdUniqueness(dir+"/organisation_0_0.csv", 0);
    }

    @Test
    public void placeTest() {
        testIdUniqueness(dir+"/place_0_0.csv", 0);
    }

    @Test
    public void tagTest() {
        testIdUniqueness(dir+"/tag_0_0.csv", 0);
    }

    @Test
    public void tagclassTest() {
        testIdUniqueness(dir+"/tagclass_0_0.csv", 0);
    }

    @Test
    public void personKnowsPersonTest() {
        testPairUniquenessPlusExistance(dir+"/person_knows_person_0_0.csv",0,1,dir+"/person_0_0.csv",0);
    }

    @Test
    public void organisationIsLocatedInPlaceTest() {
        testPairUniquenessPlusExistance(dir+"/organisation_isLocatedIn_place_0_0.csv",0,1,dir+"/organisation_0_0.csv",0,dir+"/place_0_0.csv",0);
    }

    @Test
    public void placeIsPartOfPlaceTest() {
        testPairUniquenessPlusExistance(dir+"/place_isPartOf_place_0_0.csv",0,1,dir+"/place_0_0.csv",0);
    }

    @Test
    public void tagClassIsSubclassOfTest() {
        testPairUniquenessPlusExistance(dir+"/tagclass_isSubclassOf_tagclass_0_0.csv",0,1,dir+"/tagclass_0_0.csv",0);
    }

    @Test
    public void tagHasTypeTagclassCheck() {
        testPairUniquenessPlusExistance(dir+"/tag_hasType_tagclass_0_0.csv",0,1,dir+"/tag_0_0.csv",0,dir+"/tagclass_0_0.csv",0);
    }

    @Test
    public void personStudyAtOrganisationCheck() {
        testPairUniquenessPlusExistance(dir+"/person_studyAt_organisation_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/organisation_0_0.csv",0);
    }

    @Test
    public void personWorkAtOrganisationCheck() {
        testPairUniquenessPlusExistance(dir+"/person_workAt_organisation_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/organisation_0_0.csv",0);
    }

    @Test
    public void personHasInterestTagCheck() {
        testPairUniquenessPlusExistance(dir+"/person_hasInterest_tag_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/tag_0_0.csv",0);
    }

    @Test
    public void personIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistance(dir+"/person_isLocatedIn_place_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/place_0_0.csv",0);
    }

    @Test
    public void forumHasTagCheck() {
        testPairUniquenessPlusExistance(dir+"/forum_hasTag_tag_0_0.csv",0,1,dir+"/forum_0_0.csv",0,dir+"/tag_0_0.csv",0);
    }

    @Test
    public void forumHasModeratorPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/forum_hasModerator_person_0_0.csv",0,1,dir+"/forum_0_0.csv",0,dir+"/person_0_0.csv",0);
    }

    @Test
    public void forumHasMemberPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/forum_hasMember_person_0_0.csv",0,1,dir+"/forum_0_0.csv",0,dir+"/person_0_0.csv",0);
    }

    @Test
    public void forumContainerOfPostCheck() {
        testPairUniquenessPlusExistance(dir+"/forum_containerOf_post_0_0.csv",0,1,dir+"/forum_0_0.csv",0,dir+"/post_0_0.csv",0);
    }

    @Test
    public void commentHasCreatorPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/comment_hasCreator_person_0_0.csv",0,1,dir+"/comment_0_0.csv",0,dir+"/person_0_0.csv",0);
    }

    @Test
    public void commentHasTagTagCheck() {
        testPairUniquenessPlusExistance(dir+"/comment_hasTag_tag_0_0.csv",0,1,dir+"/comment_0_0.csv",0,dir+"/tag_0_0.csv",0);
    }

    @Test
    public void commentIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistance(dir+"/comment_isLocatedIn_place_0_0.csv",0,1,dir+"/comment_0_0.csv",0,dir+"/place_0_0.csv",0);
    }

    @Test
    public void commentReplyOfCommentCheck() {
        testPairUniquenessPlusExistance(dir+"/comment_replyOf_comment_0_0.csv",0,1,dir+"/comment_0_0.csv",0,dir+"/comment_0_0.csv",0);
    }

    @Test
    public void commentReplyOfPostCheck() {
        testPairUniquenessPlusExistance(dir+"/comment_replyOf_post_0_0.csv",0,1,dir+"/comment_0_0.csv",0,dir+"/post_0_0.csv",0);
    }

    @Test
    public void postHasCreatorPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/post_hasCreator_person_0_0.csv",0,1,dir+"/post_0_0.csv",0,dir+"/person_0_0.csv",0);
    }

    @Test
    public void postIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistance(dir+"/post_isLocatedIn_place_0_0.csv",0,1,dir+"/post_0_0.csv",0,dir+"/place_0_0.csv",0);
    }

    @Test
    public void personLikesCommentCheck() {
        testPairUniquenessPlusExistance(dir+"/person_likes_comment_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/comment_0_0.csv",0);
    }

    @Test
    public void personLikesPostCheck() {
        testPairUniquenessPlusExistance(dir+"/person_likes_post_0_0.csv",0,1,dir+"/person_0_0.csv",0,dir+"/post_0_0.csv",0);
    }

    // test update stream  time consistency
    @Test
    public void updateStreamForumsConsistencyCheck() {
        testLongPair(dir+"/updateStream_0_0_forum.csv",0,1,NumericPairCheck.NumericCheckType.G);
    }

    @Test
    public void queryParamsTest() {
        LongParser parser = new LongParser();
        ColumnSet<Long> persons = new ColumnSet<Long>(parser,new File(dir+"/person_0_0.csv"),0,1);
        persons.initialize();
        List<ColumnSet<Long>> personsRef = new ArrayList<ColumnSet<Long>>();
        personsRef.add(persons);
        List<Integer> personIndex = new ArrayList<Integer>();
        personIndex.add(0);
        ExistsCheck<Long> existsPersonCheck = new ExistsCheck<Long>(parser,personIndex, personsRef);

        FileChecker fileChecker = new FileChecker(sdir+"/query_1_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 1 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_2_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 2 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_3_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 3 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_4_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 4 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_5_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 5 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_6_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 6 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_7_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 7 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_8_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 8 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_9_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 9 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_10_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 10 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_11_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 11 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_12_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 12 PERSON EXISTS ",true, false);

        personIndex.add(1);
        ExistsCheck<Long> exists2PersonCheck = new ExistsCheck<Long>(parser,personIndex, personsRef);

        fileChecker = new FileChecker(sdir+"/query_13_param.txt");
        fileChecker.addCheck(exists2PersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 13 PERSON EXISTS ",true, false);

        fileChecker = new FileChecker(sdir+"/query_14_param.txt");
        fileChecker.addCheck(exists2PersonCheck);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST QUERY 14 PERSON EXISTS ",true, false);


    }
    // test query parameters correctness
    // query 1, check person id existance and surname existance
    // query 2, check person id existance and date time within simulation interval
    // query 3, check person id existance, country X and Y existance, startData + duration within simulation interval
    // query 4, check person id existance and startDate + duration within simulation interval
    // query 5, check person id and date within simulation interval
    // query 6, check person id and tag name existance
    // query 7, check person id existance
    // query 8, check person id existance
    // query 9, check person id and date within simulation interval
    // query 10, check person id existance and month between 1 and 12
    // query 11, check person id existance, Country existance and year something reasonable
    // query 12, check person id existance and tagclass existance
    // query 13, check persons id existance
    // query 14, check persons id existance

    public void testLongPair(String fileName, Integer columnA, Integer columnB, NumericPairCheck.NumericCheckType type) {
        FileChecker fileChecker = new FileChecker(fileName);
        LongParser parser = new LongParser();
        LongPairCheck check = new LongPairCheck(parser, " Long check less equal ", columnA, columnB, type);
        fileChecker.addCheck(check);
        if(!fileChecker.run(0)) assertEquals("ERROR PASSING TEST LONG PAIR FOR FILE "+fileName,true, false);
    }

    public void testIdUniqueness(String fileName, int column) {
        FileChecker fileChecker = new FileChecker(fileName);
        UniquenessCheck check = new UniquenessCheck(column);
        fileChecker.addCheck(check);
        if(!fileChecker.run(1)) assertEquals("ERROR PASSING TEST ID UNIQUENESS FOR FILE "+fileName,true, false);
    }

    public void testPairUniquenessPlusExistance(String relationFileName, int columnA, int columnB, String entityFileNameA, int entityColumnA, String entityFileNameB, int entityColumnB) {
        LongParser parser = new LongParser();
        ColumnSet<Long> entitiesA = new ColumnSet<Long>(parser,new File(entityFileNameA),entityColumnA,1);
        entitiesA.initialize();
        ColumnSet<Long> entitiesB = new ColumnSet<Long>(parser,new File(entityFileNameB),entityColumnB,1);
        entitiesB.initialize();
        FileChecker fileChecker = new FileChecker(relationFileName);
        PairUniquenessCheck pairUniquenessCheck = new PairUniquenessCheck<Long,Long>(parser,parser,columnA,columnB);
        fileChecker.addCheck(pairUniquenessCheck);
        List<ColumnSet<Long>> entityARefColumns = new ArrayList<ColumnSet<Long>>();
        entityARefColumns.add(entitiesA);
        List<ColumnSet<Long>> entityBRefColumns = new ArrayList<ColumnSet<Long>>();
        entityBRefColumns.add(entitiesB);
        List<Integer> organisationIndices = new ArrayList<Integer>();
        organisationIndices.add(columnA);
        List<Integer> placeIndices = new ArrayList<Integer>();
        placeIndices.add(columnB);
        ExistsCheck<Long> existsEntityACheck = new ExistsCheck<Long>(parser,organisationIndices, entityARefColumns);
        ExistsCheck<Long> existsEntityBCheck = new ExistsCheck<Long>(parser,placeIndices, entityBRefColumns);
        fileChecker.addCheck(existsEntityACheck);
        fileChecker.addCheck(existsEntityBCheck);
        assertEquals("ERROR PASSING ORGANISATION_ISLOCATEDIN_PLACE TEST",true, fileChecker.run(1));

    }

    public void testPairUniquenessPlusExistance(String relationFileName, int columnA, int columnB, String entityFileName, int entityColumn) {
        LongParser parser = new LongParser();
        ColumnSet<Long> entities = new ColumnSet<Long>(parser,new File(entityFileName),entityColumn,1);
        entities.initialize();
        FileChecker fileChecker = new FileChecker(relationFileName);
        PairUniquenessCheck pairUniquenessCheck = new PairUniquenessCheck<Long,Long>(parser,parser,columnA,columnB);
        fileChecker.addCheck(pairUniquenessCheck);
        List<ColumnSet<Long>> refcolumns = new ArrayList<ColumnSet<Long>>();
        refcolumns.add(entities);
        List<Integer> columnIndices = new ArrayList<Integer>();
        columnIndices.add(columnA);
        columnIndices.add(columnB);
        ExistsCheck existsCheck = new ExistsCheck<Long>(parser,columnIndices, refcolumns);
        fileChecker.addCheck(existsCheck);
        assertEquals("ERROR PASSING "+relationFileName+" TEST",true, fileChecker.run(1));
    }

}