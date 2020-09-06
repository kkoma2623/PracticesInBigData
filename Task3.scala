package bigdata

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Task3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Task3")
    val sc = new SparkContext(conf);

    val input = args(0)
    val output = args(1)

    val sortedEdges = sc.textFile(input)repartition(120) // 0 1

    val edges1 = sortedEdges.map{x=>
      val split = x.split("\t")
      (split(0).toInt, split(1).toInt)
    }.sortByKey()

    val edges2 = sortedEdges.map{x=>
      val split = x.split("\t")
      (split(1).toInt, split(0).toInt)na
    }.sortByKey()

    val edges = edges1.union(edges2)

    val degrees = edges.map{x=>
      (x._1, 1)
    }.reduceByKey{(a,b)=>
      (a+b)
    }.filter(x=> x._2 > 1).sortByKey()

    val edgesDegrees = edges1.join(degrees).map{x=>
      (x._2._1, (x._1, x._2._2))
    }.join(degrees).map{x=>
      //      step 3-1
      val u =x._2._1._1
      val v = x._1
      val du = x._2._1._2
      val dv = x._2._2
      if(du < dv || (du == dv && u < v)) (u,v)
      else (v,u)
    }.groupByKey().flatMap{x=>
      val sortedEdges = x._2.toArray.sortWith(_<_)
      val size = sortedEdges.size
      var result = Array[Tuple2[Tuple2[Int, Int], Int]]()

      for(i<- 0 until size-1){
        for(j<- i+1 until size){
          result = result ++ Array(((sortedEdges(i),sortedEdges(j)), x._1))
        }
      }
      (result)
    }.sortByKey()

    val forCount = edges1.map{x=>
      ((x._1, x._2), None)
    }
    val edgesMakeTriangle = edgesDegrees.join(forCount).flatMap{x=>
      val u = x._2._1
      val v = x._1._1
      val w = x._1._2
      Array((u,1), (v,1), (w,1))
    }.reduceByKey{(a,b)=>
      (a+b)
    }.sortByKey()


    val res = edges1.flatMap{x=>
      Array((x._1, None), (x._2, None))
    }.reduceByKey{(a,b)=>
      None
    }.join(edgesMakeTriangle).map{x=>
      (x._1, x._2._2)
    }.sortByKey()

    val triangle = res.map{x=>
      (x._1 + "\t" + x._2)
    }

    triangle.saveAsTextFile(output)
  }
}