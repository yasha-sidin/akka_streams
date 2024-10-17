import akka.{Done, NotUsed, actor}
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Props, SpawnProtocol}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source}
import akka.util.Timeout
import akka_typed.TypedCalculatorWriteSide.{Add, Added, Command, Divide, Divided, Multiplied, Multiply}
import scalikejdbc.DB.using
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, DB}
import akka_typed.CalculatorRepository.{getLatestOffsetAndResult, getSlickSession, insertionFromSource}
import akka_typed.{Supervisor, TypedCalculatorReadSide, TypedCalculatorWriteSide, persId}
import slick.jdbc.GetResult

import java.util.UUID
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

trait CborSerialization

object akka_typed {

  object Supervisor {
    def apply(): Behavior[SpawnProtocol.Command] = Behaviors.setup { ctx =>
      ctx.log.info(ctx.self.toString)
      SpawnProtocol()
    }
  }

  val persId: PersistenceId = PersistenceId.ofUniqueId("001")

  object CalculatorTags {
    val Added = "added"
    val Multiplied = "multiplied"
    val Divided = "divided"
  }

  object TypedCalculatorWriteSide {
    sealed trait Command

    case class Add(amount: Int) extends Command

    case class Multiply(amount: Int) extends Command

    case class Divide(amount: Int) extends Command

    sealed trait Event

    case class Added(id: Int, amount: Int) extends Event

    case class Multiplied(id: Int, amount: Int) extends Event

    case class Divided(id: Int, amount: Int) extends Event

    final case class State(value: Int) extends CborSerialization {
      def add(amount: Int): State = copy(value = value + amount)

      def multiply(amount: Int): State = copy(value = value * amount)

      def divide(amount: Int): State = copy(value = value / amount)
    }

    object State {
      val empty: State = State(0)
    }

    private def handleCommand(
                               persistenceId: String,
                               state: State,
                               command: Command,
                               ctx: ActorContext[Command]
                             ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding  for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
            .persist(added)
            .thenRun {
              x => ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiplying  for number: $amount and state is ${state.value}")
          val multiplied = Multiplied(persistenceId.toInt, amount)
          Effect
            .persist(multiplied)
            .thenRun {
              x => ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive dividing  for number: $amount and state is ${state.value}")
          val divided = Divided(persistenceId.toInt, amount)
          Effect
            .persist(divided)
            .thenRun {
              x => ctx.log.info(s"The state result is ${x.value}")
            }
      }

    private def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling event Added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling event Multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling event Divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(persistenceId: PersistenceId): Behavior[Command] =
      Behaviors.setup { ctx =>
        ctx.log.info("Calculator starting...")
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persistenceId,
          State.empty,
          (state, command) => handleCommand(persistenceId.id, state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        ).withTagger {
          case Added(_, _) => Set(CalculatorTags.Added)
          case Multiplied(_, _) => Set(CalculatorTags.Multiplied)
          case Divided(_, _) => Set(CalculatorTags.Divided)
        }
      }

  }

  case class TypedCalculatorReadSide(system: ActorSystem[SpawnProtocol.Command], slickSession: SlickSession) {
    implicit val s: ActorSystem[SpawnProtocol.Command] = system
    implicit val session: SlickSession = slickSession
    implicit val ex: ExecutionContextExecutor = system.executionContext
    getLatestOffsetAndResult.flatMap { entityOption =>
      val startOffset = entityOption.map(_.writeSideOffset).getOrElse(0L)
      val readJournal: CassandraReadJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)
      val entitySource = source
        .map {
          event =>
            event.event match {
              case Added(_, amount) =>
                Await.result(getLatestOffsetAndResult.map { opt =>
                  val lastRes = opt.map(_.calculatedValue).getOrElse(0.0)
                  val newResult = lastRes + amount
                  println(s"Log from Added: $newResult")
                  CalculatorEntity(calculatedValue = newResult, writeSideOffset = event.sequenceNr)
                }, 2 seconds)
              case Multiplied(_, amount) =>
                Await.result(getLatestOffsetAndResult.map { opt =>
                  val lastRes = opt.map(_.calculatedValue).getOrElse(0.0)
                  val newResult = lastRes * amount
                  println(s"Log from Added: $newResult")
                  CalculatorEntity(calculatedValue = newResult, writeSideOffset = event.sequenceNr)
                }, 2 seconds)
              case Divided(_, amount) =>
                Await.result(getLatestOffsetAndResult.map { opt =>
                  val lastRes = opt.map(_.calculatedValue).getOrElse(0.0)
                  val newResult = lastRes / amount
                  println(s"Log from Added: $newResult")
                  CalculatorEntity(calculatedValue = newResult, writeSideOffset = event.sequenceNr)
                }, 2 seconds)
            }
        }
      insertionFromSource(entitySource)
    }
    /*
      // homework, spoiler
        def updateState(event: Any, seqNum: Long): Result ={
          val newStste = event match {
            case Added(_amount)=>
              ???
            case Multiplied(_,amount)=>
              ???
            case Divided(_amount)=>
              ???
          }
          Result(newState, seqNum)
        }

        val graph = GraphDSL.create(){
          implicit builder: GraphDSL.Builder[NotUsed] =>
            //1.
            val input = builder.add(source)
            val stateUpdater = builder.add(Flow[EventEnvelope].map(e=> updateState(e.event, e.sequenceNr)))
            val localSaveOutput = builder.add(Sink.foreach[Result]{
              r=>
                latestCalculatedResult = r.state
                println("something to print")
            })

            val dbSaveOutput = builder.add(
              Slick.sink[Result](r=> updatedResultAndOffset(r))
            )

            // надо разделить builder на 2  c помощью Broadcats
            //см https://blog.rockthejvm.com/akka-streams-graphs/

            //надо будет сохранить flow(разделенный на 2) в localSaveOutput и dbSaveOutput
            //в конце закрыть граф и запустить его RunnableGraph.fromGraph(graph).run()




        }*/
  }

  case class CalculatorEntity(id: String = UUID.randomUUID().toString, calculatedValue: Double, writeSideOffset: Long)

  object CalculatorRepository {
    implicit val getUserResult: GetResult[CalculatorEntity] = GetResult(r => CalculatorEntity(r.nextString(), r.nextDouble(), r.nextLong()))

    def getSlickSession: SlickSession = SlickSession.forConfig("slick-postgres")

    def insertionFromSource(source: Source[CalculatorEntity, NotUsed])(implicit slickSession: SlickSession,
                                                                       actorSystem: ActorSystem[SpawnProtocol.Command]): Future[Done] = {
      import slickSession.profile.api._
      source.runWith {
        Slick.sink(calc => sqlu"INSERT INTO public.result VALUES (${calc.id}, ${calc.calculatedValue},${calc.writeSideOffset})")
      }
    }

    def getLatestOffsetAndResult(implicit slickSession: SlickSession,
                                  actorSystem: ActorSystem[SpawnProtocol.Command]): Future[Option[CalculatorEntity]] = {
      import slickSession.profile.api._
      Slick
        .source(sql"SELECT id, calculated_value, write_side_offset FROM public.result ORDER BY write_side_offset DESC LIMIT 1".as[CalculatorEntity])
        .runWith(Sink.headOption)
    }
  }
}

object write {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[SpawnProtocol.Command] = ActorSystem(Supervisor(), "CalculatorApp")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext
    implicit val timeout: Timeout = Timeout(1 seconds)

    val calculatorRef: Future[ActorRef[Command]] =
      system.ask[ActorRef[Command]](SpawnProtocol.Spawn[Command](TypedCalculatorWriteSide(persId), "Calculator", Props.empty, _))

    calculatorRef.foreach { ref =>
      ref ! Add(10)
      ref ! Add(4)
      ref ! Multiply(1848)
      ref ! Divide(2)
      ref ! Multiply(6655)
      ref ! Add(1345)
      ref ! Add(-798)
    }
  }
}

object synchronizeReadSideWithWrite {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[SpawnProtocol.Command] = ActorSystem(Supervisor(), "CalculatorApp")
    implicit val session: SlickSession = getSlickSession
    TypedCalculatorReadSide(system, session)
  }
}