//package org.apache.flink.quickstart
//
//
//import org.apache.flink.api.common.operators.Order
//import org.apache.flink.api.scala._
//import org.apache.flink.ml.common._
//import org.apache.flink.ml.math.DenseVector
//import org.apache.flink.ml.regression.MultipleLinearRegression
//
//
//
//object CMAPSSJob {
//  def main(args: Array[String]) {
//
//    // input files
//    val trainingFile = "/home/hemsen/Desktop/CMAPSSData/train_FD001.txt"
//    val testingFile  = "/home/hemsen/Desktop/CMAPSSData/test_FD001.txt"
//    val rulFile      = "/home/hemsen/Desktop/CMAPSSData/RUL_FD001.txt"
//
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.disableSysoutLogging;
//
//    val trainingData = env.readCsvFile[(CMAPSSData)](
//        trainingFile,
//      fieldDelimiter = " ",
//      pojoFields = Array("id", "cycle", "settings1", "settings2", "settings2", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10",
//        "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21"))
//
//    val groupedMaxCycle = trainingData.map { x => (x.id, x.cycle) }.groupBy(0).max(1)
//
//    val labeledTrainingData = trainingData.join(groupedMaxCycle).where("id").equalTo(0).map { x => (x._1, x._2._2 - x._1.cycle) }.sortPartition("_1.id", Order.ASCENDING).sortPartition("_1.cycle", Order.ASCENDING)
//
//    val trainingDataLV = labeledTrainingData
//      .map { tuple =>
//          LabeledVector(tuple._2.toDouble, DenseVector(tuple._1.features.slice(0, tuple._1.features.length).toArray))
//    }
//
//    val testData = env.readCsvFile[(CMAPSSData)](
//        testingFile,
//      fieldDelimiter = " ",
//      pojoFields = Array("id", "cycle", "settings1", "settings2", "settings2", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10",
//        "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21"))
//
//    val testDataLV = testData
//      .map { x =>
//        DenseVector(x.features.slice(0, x.features.length).toArray)
//    }
//
//    testDataLV.print
//
//    // clean-up needed, because there is a whitespace after the integers (e.g. "100 ")
//    val groundTruthData = env.readTextFileWithValue(rulFile)
//
//    groundTruthData.print
//
//    val mlr = MultipleLinearRegression()
//      .setStepsize(0.1)
//      .setIterations(1000)
//      .setConvergenceThreshold(0.001)
//
//
//    mlr.fit(trainingDataLV)
//
//    val predictions = mlr.predict(testDataLV)
//
//    predictions.print
//
//    val squaredResidualSum = mlr.squaredResidualSum(trainingDataLV)
//
//    squaredResidualSum.print
//
//    // execute program
//    env.execute("Flink Scala API Skeleton")
//  }
//
//  class CMAPSSData {
//
//    var id: Int = _
//    var cycle: Int = _
//    var settings1: Double = _
//    var settings2: Double = _
//    var settings3: Double = _
//    var s1: Double = _
//    var s2: Double = _
//    var s3: Double = _
//    var s4: Double = _
//    var s5: Double = _
//    var s6: Double = _
//    var s7: Double = _
//    var s8: Double = _
//    var s9: Double = _
//    var s10: Double = _
//    var s11: Double = _
//    var s12: Double = _
//    var s13: Double = _
//    var s14: Double = _
//    var s15: Double = _
//    var s16: Double = _
//    var s17: Double = _
//    var s18: Double = _
//    var s19: Double = _
//    var s20: Double = _
//    var s21: Double = _
//
//    def features : List[Double] = {
//      // "sensor15" "sensor17" "sensor20" "sensor21"
//      val a1 = List(id.toDouble, cycle.toDouble, settings1, settings2, s2,s3,s4,s6,s7,s8,s9,s11,s12,s13,s14,s15, s17, s20, s21)
//      return a1
//    }
//
//    def features1 : List[Double] = {
//      val a1 = List(id.toDouble, cycle.toDouble, settings1, settings2, settings3, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21)
//      return a1
//    }
//
//    override def toString = "CMAPSSData [id=" + id + ", cycle=" + cycle + ", settings1=" + settings1 + ", settings2=" + settings2 + ", settings3=" + settings3 + ", s1=" +
//      s1 + ", s2=" + s2 + ", s3=" + s3 + ", s4=" + s4 + ", s5=" + s5 + ", s6=" + s6 + ", s7=" + s7 + ", s8=" + s8 + ", s9=" + s9 + ", s10=" + s10 + ", s11=" + s11 + ", s12=" +
//      s12 + ", s13=" + s13 + ", s14=" + s14 + ", s15=" + s15 + ", s16=" + s16 + ", s17=" + s17 + ", s18=" + s18 + ", s19=" + s19 + ", s20=" + s20 + ", s21=" + s21 + "]";
//
//  }
//
//}