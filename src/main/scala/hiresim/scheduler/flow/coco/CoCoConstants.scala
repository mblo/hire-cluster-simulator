package hiresim.scheduler.flow.coco

object CoCoConstants {

  val Scaling_Factor: Double = 0.2

  val PRECISION: Long = 100L


  // *********** Penalty values *************** //

  val Penalty_Turtle_Any: Int = 50


  /** Sheep Penalties * */

  val Penalty_Sheep_Turtle: Int = 10

  val Penalty_Sheep_Sheep: Int = 50

  val Penalty_Sheep_Rabbit: Int = 100

  val Penalty_Sheep_Devil: Int = 200


  /** Rabbit Penalties * */

  val Penalty_Rabbit_Turtle: Int = 10

  val Penalty_Rabbit_Sheep: Int = 50

  val Penalty_Rabbit_Rabbit: Int = 200

  val Penalty_Rabbit_Devil: Int = 1000


  /** Devil Penalties * */

  val Penalty_Devil_Turtle: Int = 10

  val Penalty_Devil_Sheep: Int = 200

  val Penalty_Devil_Rabbit: Int = 1000

  val Penalty_Devil_Devil: Int = 200


}
