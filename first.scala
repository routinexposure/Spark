import org.apache.spark.sql.SparkSession

object CountLetterB {
  def main(args: Array[String]): Unit = {
    // Создание объекта SparkSession
    val spark = SparkSession.builder
      .appName("CountLetterB")
      .master("local")
      .getOrCreate()

    try {
      // Загрузка текста из файла и подсчет букв "b"
      val inputRDD = spark.sparkContext.textFile("/Users/madi/Desktop/madi/war_and_peace.txt")
      val countLetterB = inputRDD.flatMap(line => line.toLowerCase().filter(_ == 'b')).count()

      println(s"Количество букв 'b' в книге: $countLetterB")
    } finally {
      // Остановка SparkSession
      spark.stop()
    }
  }
}

CountLetterB.main(Array())



import org.apache.spark.sql.SparkSession

object CountLetterC {
  def main(args: Array[String]): Unit = {
    // Создание объекта SparkSession
    val spark = SparkSession.builder
      .appName("CountLetterC")
      .master("local")
      .getOrCreate()

    try {
      // Загрузка текста из файла и подсчет букв "c"
      val inputRDD = spark.sparkContext.textFile("/Users/madi/Desktop/madi/war_and_peace.txt")
      val countLetterC = inputRDD.flatMap(line => line.toLowerCase().filter(_ == 'c')).count()

      println(s"Количество букв 'c' в книге: $countLetterC")
    } finally {
      // Остановка SparkSession
      spark.stop()
    }
  }
}

CountLetterC.main(Array())




import org.apache.spark.sql.SparkSession

object CountLetterD {
  def main(args: Array[String]): Unit = {
    // Создание объекта SparkSession
    val spark = SparkSession.builder
      .appName("CountLetterD")
      .master("local")
      .getOrCreate()

    try {
      // Загрузка текста из файла и подсчет буквы "D"
      val inputRDD = spark.sparkContext.textFile("/Users/madi/Desktop/madi/war_and_peace.txt")
      val countLetterD = inputRDD.flatMap(line => line.toLowerCase().filter(_ == 'd')).count()

      println(s"Количество букв 'D' в книге: $countLetterD")
    } finally {
      // Остановка SparkSession
      spark.stop()
    }
  }
}

CountLetterD.main(Array())



import org.apache.spark.sql.SparkSession

object CountLetterA {
  def main(args: Array[String]): Unit = {
    // Создание объекта SparkSession
    val spark = SparkSession.builder
      .appName("CountLetterA")
      .master("local")
      .getOrCreate()

    try {
      // Загрузка текста из файла и подсчет буквы "a"
      val inputRDD = spark.sparkContext.textFile("/Users/madi/Desktop/madi/war_and_peace.txt")
      val countLetterA = inputRDD.flatMap(line => line.toLowerCase().filter(_ == 'a')).count()

      println(s"Количество букв 'a' в книге: $countLetterA")
    } finally {
      // Остановка SparkSession
      spark.stop()
    }
  }
}

CountLetterA.main(Array())

import org.apache.spark.sql.SparkSession

object CountDot {
  def main(args: Array[String]): Unit = {
    // Создание объекта SparkSession
    val spark = SparkSession.builder
      .appName("CountDot")
      .master("local")
      .getOrCreate()

    try {
      // Загрузка текста из файла и подсчет точек
      val inputRDD = spark.sparkContext.textFile("/Users/madi/Desktop/madi/war_and_peace.txt")
      val countDot = inputRDD.flatMap(line => line.filter(_ == '.')).count()

      println(s"Количество точек '.' в книге: $countDot")
    } finally {
      // Остановка SparkSession
      spark.stop()
    }
  }
}


CountDot.main(Array())