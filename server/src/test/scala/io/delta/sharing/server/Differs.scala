package io.delta.sharing.server

import difflicious._
import difflicious.differ._
import difflicious.generic.auto._
import difflicious.implicits.toPairByOps
import difflicious.utils.TypeName
import io.delta.sharing.client.{model => clientModel}

import scala.collection.immutable.ListMap
import scala.reflect.ClassTag
import scala.reflect.runtime.{currentMirror => cm}
import scala.reflect.runtime.universe._

object Differs {
  implicit val protocolDiffer: Differ[clientModel.Protocol] = Differ.derived
  implicit val formatDiffer: Differ[clientModel.Format] = Differ.derived
  implicit val longDiffer: Differ[java.lang.Long] = Differ.useEquals[java.lang.Long](l => if (l == null) "null" else l.toString)
  implicit val metadataDiffer: Differ[clientModel.Metadata] =
    Differ.derived[clientModel.Metadata].ignoreAt(_.id).ignoreAt(_.name) // TODO: Fix name
  implicit val addFileDiffer: Differ[clientModel.AddFile] =
    Differ.derived[clientModel.AddFile].ignoreAt(_.size).ignoreAt(_.id).ignoreAt(_.stats).ignoreAt(_.expirationTimestamp)
  implicit val addFileForCdfDiffer: Differ[clientModel.AddFileForCDF] =
    Differ.derived[clientModel.AddFileForCDF].ignoreAt(_.id).ignoreAt(_.size).ignoreAt(_.stats).ignoreAt(_.timestamp).ignoreAt(_.expirationTimestamp) // TODO: Fix stats
  implicit val addCdcFileDiffer: Differ[clientModel.AddCDCFile] =
    Differ.derived[clientModel.AddCDCFile].ignoreAt(_.size)
  implicit val addFileSeqDiffer: Differ[Seq[clientModel.AddFile]] =
    Differ.seqDiffer[Seq, clientModel.AddFile].pairBy(_.url)
  implicit val addFileForCDFSeqDiffer: Differ[Seq[clientModel.AddFileForCDF]] =
    Differ.seqDiffer[Seq, clientModel.AddFileForCDF].pairBy(_.url)
  implicit val addCdcFileSeqDiffer: Differ[Seq[clientModel.AddCDCFile]] =
    Differ.seqDiffer[Seq, clientModel.AddCDCFile].pairBy(_.url)
  implicit val removeFileDiffer: Differ[clientModel.RemoveFile] =
    Differ.derived[clientModel.RemoveFile].ignoreAt(_.url)
  implicit val deltaTableFilesDiffer: Differ[clientModel.DeltaTableFiles] =
    Differ.derived[clientModel.DeltaTableFiles].ignoreAt(_.version).ignoreAt(_.refreshToken).ignoreAt(_.additionalMetadatas)
}
