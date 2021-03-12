package models

case class Customer(customer_id: String, cname: String, age: Int)
case class LegalCustomer(customer_id:String,cname:String,age:Int,isLegal:Boolean)
