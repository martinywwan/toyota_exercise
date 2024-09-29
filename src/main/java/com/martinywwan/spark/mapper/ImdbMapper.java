//package com.martinywwan.spark.mapper;
////
////import com.martinywwan.model.NameBasic;
////import org.apache.spark.api.java.function.Function;
////import org.springframework.stereotype.Component;
////
////public class ImdbMapper {
////
////    public static final Function nameBasicMapper = (Function<String, NameBasic>) line -> {
////        try {
////            String[] parts = line.split("\\t"); //tab separated values
////            String nameId = parts[0];
////            String primaryName = parts[1];
////            String birthYear = parts[2];
////            String deathYear = parts[3].equals("\\N") ? null : parts[3];
////            String primaryProfession = parts[4];
////            String knownForTitles = parts[5];
////            NameBasic nameBasic = new NameBasic(nameId, primaryName, birthYear, deathYear, primaryProfession, knownForTitles);
////            return nameBasic;
////        } catch (Exception e){
////            System.out.println("In a crtical environment, we would alert if there is bad data and store in an error store");
////        }
////        // return a null rather than an empty object
////        return null;
////    };
////
////
////}
//
//import com.martinywwan.model.NameBasic;
//import org.apache.spark.api.java.function.MapFunction;
//import org.apache.spark.sql.Row;
//
//public class ImdbMapper implements MapFunction<NameBasic, Row> {
//
//    @Override
//    public Row call(Customer customer) throws Exception {
//        Row row = RowFactory.create(
//                customer.getId(),
//                customer.getName().toUpperCase(),
//                StringUtils.substring(customer.getGender(),0, 1),
//                customer.getTransaction_amount()
//        );
//        return row;
//    }
//
//    @Override
//    public Row call(NameBasic nameBasic) throws Exception {
//        return null;
//    }
//}