import { FileMetadata } from '../pod/types'
import { assertAccount } from '../account/utils'
import { assertPodName, getExtendedPodsListByAccountData, getSharedPodInfo, META_VERSION } from '../pod/utils'
import { getUnixTimestamp } from '../utils/time'
import { stringToBytes, wrapBytesWithHelpers } from '../utils/bytes'
import { AccountData } from '../account/account-data'
import {
  assertFullPathWithName,
  createFileShareInfo,
  extractPathInfo,
  getSharedFileInfo,
  updateFileMetadata,
  uploadBytes,
} from './utils'
import { writeFeedData } from '../feed/api'
import { downloadData, generateBlockName } from './handler'
import { blocksToManifest, getFileMetadataRawBytes, rawFileMetadataToFileMetadata } from './adapter'
import { Blocks, DataUploadOptions, FileReceiveOptions, FileShareInfo } from './types'
import { addEntryToDirectory, removeEntryFromDirectory } from '../content-items/handler'
import { Data, Reference } from '@ethersphere/bee-js'
import { getRawMetadata } from '../content-items/utils'
import { assertRawFileMetadata, combine } from '../directory/utils'
import { assertEncryptedReference, EncryptedReference } from '../utils/hex'
import { prepareEthAddress } from '../utils/address'

/**
 * Files management class
 */
export class File {
  public readonly defaultUploadOptions: DataUploadOptions = {
    blockSize: 1000000,
    contentType: '',
  }

  constructor(private accountData: AccountData) {}

  /**
   * Downloads file content
   *
   * @param podName pod where file is stored
   * @param fullPath full path of the file
   */
  async downloadData(podName: string, fullPath: string): Promise<Data> {
    assertAccount(this.accountData)
    assertPodName(podName)
    assertFullPathWithName(fullPath)
    assertPodName(podName)

    return downloadData(
      this.accountData.connection.bee,
      fullPath,
      (await getExtendedPodsListByAccountData(this.accountData, podName)).podAddress,
      this.accountData.connection.options?.downloadOptions,
    )
  }

  /**
   * Uploads file content
   *
   * @param podName pod where file is stored
   * @param fullPath full path of the file
   * @param data file content
   * @param options upload options
   */
  async uploadData(
    podName: string,
    fullPath: string,
    data: Uint8Array | string,
    options?: DataUploadOptions,
  ): Promise<FileMetadata> {
    options = { ...this.defaultUploadOptions, ...options }
    assertAccount(this.accountData)
    assertPodName(podName)
    assertFullPathWithName(fullPath)
    assertPodName(podName)
    data = typeof data === 'string' ? stringToBytes(data) : data
    const connection = this.accountData.connection
    const extendedInfo = await getExtendedPodsListByAccountData(this.accountData, podName)
    const pathInfo = extractPathInfo(fullPath)
    const now = getUnixTimestamp()
    const blocksCount = Math.ceil(data.length / options.blockSize)
    const blocks: Blocks = { blocks: [] }
    for (let i = 0; i < blocksCount; i++) {
      const currentBlock = data.slice(i * options.blockSize, (i + 1) * options.blockSize)
      const result = await uploadBytes(connection, currentBlock)
      blocks.blocks.push({
        name: generateBlockName(i),
        size: currentBlock.length,
        compressedSize: currentBlock.length,
        reference: result.reference,
      })
    }

    const manifestBytes = stringToBytes(blocksToManifest(blocks))
    const blocksReference = (await uploadBytes(connection, manifestBytes)).reference
    const meta: FileMetadata = {
      version: META_VERSION,
      podAddress: extendedInfo.podAddress,
      podName,
      filePath: pathInfo.path,
      fileName: pathInfo.filename,
      fileSize: data.length,
      blockSize: options.blockSize,
      contentType: options.contentType,
      compression: '',
      creationTime: now,
      accessTime: now,
      modificationTime: now,
      blocksReference,
    }

    await addEntryToDirectory(connection, extendedInfo.podWallet, pathInfo.path, pathInfo.filename, true)
    await writeFeedData(connection, fullPath, getFileMetadataRawBytes(meta), extendedInfo.podWallet.privateKey)

    return meta
  }

  /**
   * Deletes a file
   *
   * @param podName pod where file is located
   * @param fullPath full path of the file
   */
  async delete(podName: string, fullPath: string): Promise<void> {
    assertAccount(this.accountData)
    assertFullPathWithName(fullPath)
    assertPodName(podName)
    const pathInfo = extractPathInfo(fullPath)
    await removeEntryFromDirectory(
      this.accountData.connection,
      (
        await getExtendedPodsListByAccountData(this.accountData, podName)
      ).podWallet,
      pathInfo.path,
      pathInfo.filename,
      true,
    )
  }

  /**
   * Shares file information
   *
   * @param podName pod where file is stored
   * @param fullPath full path of the file
   */
  async share(podName: string, fullPath: string): Promise<Reference> {
    assertAccount(this.accountData)
    assertFullPathWithName(fullPath)
    assertPodName(podName)
    const connection = this.accountData.connection
    const extendedInfo = await getExtendedPodsListByAccountData(this.accountData, podName)
    const meta = (await getRawMetadata(connection.bee, fullPath, extendedInfo.podAddress)).metadata
    assertRawFileMetadata(meta)
    const data = stringToBytes(JSON.stringify(createFileShareInfo(meta, extendedInfo.podAddress)))

    return (await uploadBytes(connection, data)).reference
  }

  /**
   * Gets shared file information
   *
   * Can be executed without authentication
   *
   * @param reference encrypted swarm reference with shared file information
   *
   * @returns shared file information
   */
  async getSharedInfo(reference: string | EncryptedReference): Promise<FileShareInfo> {
    assertEncryptedReference(reference)

    return getSharedFileInfo(this.accountData.connection.bee, reference)
  }

  /**
   * Saves shared file to a personal account
   *
   * @param podName pod where file is stored
   * @param parentPath the path to the file to save
   * @param reference encrypted swarm reference with shared file information
   * @param options save options
   *
   * @returns saved file metadata
   */
  async saveShared(
    podName: string,
    parentPath: string,
    reference: string | EncryptedReference,
    options?: FileReceiveOptions,
  ): Promise<FileMetadata> {
    assertPodName(podName)
    const sharedInfo = await this.getSharedInfo(reference)
    const connection = this.accountData.connection
    const extendedInfo = await getExtendedPodsListByAccountData(this.accountData, podName)
    let meta = rawFileMetadataToFileMetadata(sharedInfo.meta)
    const fileName = options?.name ?? sharedInfo.meta.file_name
    meta = updateFileMetadata(meta, podName, parentPath, fileName, extendedInfo.podAddress)
    const fullPath = combine(parentPath, fileName)
    await addEntryToDirectory(connection, extendedInfo.podWallet, parentPath, fileName, true)
    await writeFeedData(connection, fullPath, getFileMetadataRawBytes(meta), extendedInfo.podWallet.privateKey)

    return meta
  }

  /**
   * Downloads shared file
   *
   * Can be executed without authentication
   *
   * @param fileReference encrypted swarm reference with shared file information
   */
  async downloadShared(fileReference: string | EncryptedReference): Promise<Data> {
    const info = await this.getSharedInfo(fileReference)
    const data = await downloadData(
      this.accountData.connection.bee,
      combine(info.meta.file_path, info.meta.file_name),
      prepareEthAddress(info.source_address),
      this.accountData.connection.options?.downloadOptions,
    )

    return wrapBytesWithHelpers(data)
  }

  /**
   * Downloads file from a shared pod
   *
   * Can be executed without authentication
   *
   * @param podReference encrypted swarm reference with shared pod information
   * @param fullPath full path of the file to download
   */
  async downloadFromSharedPod(podReference: string | EncryptedReference, fullPath: string): Promise<Data> {
    assertEncryptedReference(podReference)
    const info = await getSharedPodInfo(this.accountData.connection.bee, podReference)
    const data = await downloadData(
      this.accountData.connection.bee,
      fullPath,
      prepareEthAddress(info.pod_address),
      this.accountData.connection.options?.downloadOptions,
    )

    return wrapBytesWithHelpers(data)
  }
}
