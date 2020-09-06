package bigdata
import org.apache.spark.{SparkConf, SparkContext}

object Task4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Task4")
    val sc = new SparkContext(conf);
    //    val txt = sc.parallelize(Seq("0\t1", "0\t2", "1\t2", "1\t3", "1\t4", "2\t3", "0\t3"))
    val input = args(0)
    val input2 = args(1)
    val output = args(2)

    val degrees = sc.textFile(input)repartition(120) // 0 1
    val triangles = sc.textFile(input2)repartition(120) // 0 1

    val k_i = degrees.map{x=>
      val split = x.split("\t")
      (split(0), split(1))
    }
    val t_i = triangles.map{x=>
      val split = x.split("\t")
      (split(0), split(1))
    }

    val coefficient = k_i.join(t_i).map{x=>
      val t = x._2._2.toFloat
      val k = x._2._1.toFloat
      val c = t/((k*(k-1))/2)
      (x._1 + "\t" + c)
    }

    coefficient.saveAsTextFile(output)


  }
}
