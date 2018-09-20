object Utils extends App{
  def last[T](list:List[T]):T={
    list match {
      case x::Nil=>x
      case _::tail=>last(tail)
      case _=>throw new NoSuchElementException
    }
  }
  def penultimate[T](list:List[T]):T={
    list match{
      case y::Nil=>throw new NoSuchElementException
      case x::y::Nil=>x
      case _::tail=>penultimate(tail)
    }
  }
  def findKth[T](k:Int,list:List[T]):T={
    k match{
      case 0=>list.head
      case k if k>0=>findKth(k-1,list.tail)
      case _=>throw new NoSuchElementException
    }
  }

  def findKthanotherway[T](k:Int,list:List[T]):T={
    (k,list) match {
      case (0,h::_)=>h
      case (k,_::h) if(k>0)=>findKthanotherway(k-1,h)
      case _=>throw new NoSuchElementException
    }
  }
  def reverse[T](list:List[T]):List[T]={
    list match{
      case x::y=>reverse(y):::List(x)
      case Nil=>Nil
    }
  }


  def flatten[T](list:List[T]):List[T]={
    list match{
      case Nil=>Nil
      case (y:List[T])::x=>flatten(y):::x
      case (x:T)::(y:List[T])=>x::flatten(y)
    }
  }
  val l=List(1,2,3,4,5,6,7)
  print(reverse(l))
}
