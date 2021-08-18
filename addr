package net.martinprobson.spark

import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set

object AddressesDataQuestion extends App {

  case class AddressData(customerId: String, addressId: String, fromDate: Int, toDate: Int)

  case class OverlappingCustomersObject(addressId: String, fromDate: Int, toDate: Int, customerIds: Set[String])

  case class AddressGroupedData(group: Long, addressId: String, customerIds: Seq[String], startDate: Int, endDate: Int)

  case class FinalResult(customerGroupedDataList: List[AddressGroupedData], count: Int)

  def overlappingIntervals(row: ( /*addressId*/ String, List[( /*fromDate*/ Int, /*toDate*/ Int, /*customerId*/ String)])):
  HashMap[String, OverlappingCustomersObject] = {
    var counter = 1
    val hashMap = new HashMap[String, OverlappingCustomersObject]

    if (row._2.isEmpty) return hashMap

    var start = row._2(0)._1
    var end = row._2(0)._2
    val currentAddressId = row._1

    hashMap.put(currentAddressId + "," + counter, OverlappingCustomersObject(currentAddressId, start, end,
      Set(row._2(0)._3)))

    for (i <- 1 until row._2.size) {
      if (end >= row._2(i)._1) {
        if (end < row._2(i)._2) {
          // interval ends at current index so update the end
          end = row._2(i)._2
          // add the customer id of conflicting address

          val existingGroup: Option[OverlappingCustomersObject] = hashMap.get(currentAddressId + "" + counter)
          existingGroup.get.customerIds.add(row._2(i)._3)
          hashMap.put(currentAddressId + "," + counter, existingGroup.get)
        }
      } else {
        start = row._2(i)._1
        end = row._2(i)._2

        // add new entry with incremented counter id which is a new group id
        counter += 1
        hashMap.put(currentAddressId + "," + counter, OverlappingCustomersObject(currentAddressId, row._2(i)._1,
          row._2(i)._2, Set(row._2(i)._3)))
      }
    }

    hashMap
  }

  val addressLines = Source.fromFile("address_data.csv").getLines().drop(1)
  val occupancyData: List[AddressData] = addressLines.map { line =>
    val split = line.split(",")
    AddressData(split(0), split(1), split(2).toInt, split(3).toInt)
  }.toList

  val occupancyDataWithAddressGroupIds: Iterable[List[(String, OverlappingCustomersObject)]] = occupancyData
    .groupBy(row => row.addressId)
    .mapValues(_.map(r => (r.fromDate, r.toDate, r.customerId))) // map lists of occupancy data
    .mapValues(list => list.toSeq.sortBy(_._1): _*) // sort by from date ascending order
    .toMap
    .map(r => overlappingIntervals(r).toList)

  val occupancyDataWithAddressGroupIdsFlattened: List[AddressGroupedData] = occupancyDataWithAddressGroupIds.map { row =>
    row
      .map { t =>
        AddressGroupedData(t._1.split(",")(1).toLong,
          t._2.addressId, t._2.customerIds.toSeq, t._2.fromDate, t._2.toDate)
      }
  }
    .flatMap(row => row)
    .toList


//  val finalData: Map[AddressGroupedData, Int] = occupancyDataWithAddressGroupIdsFlattened.groupBy(t => t)
//    .mapValues(list => list.size)
//    .toMap

  val finalData = FinalResult(occupancyDataWithAddressGroupIdsFlattened, occupancyDataWithAddressGroupIdsFlattened.size)
}
