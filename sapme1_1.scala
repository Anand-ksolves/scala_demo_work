object HellYou extends App {
  if (args.size == 0)
    println("Hello, you")
  else
    println("Hello, " + args(0))
}

//Command-line arguments are automatically made available to you in a variable named args.
//You determine the number of elements in args with args.size (or args.length, if you prefer).
//args is an Array, and you access Array elements as args(0), args(1), etc. Because args is an object,
// you access the array elements with parentheses (not [] or any other special syntax).