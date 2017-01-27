{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Control.Concurrent.Async.Extra
    ( -- * concurrent mapping
      mapConcurrentlyBounded
    , mapConcurrentlyBounded_
    , mapConcurrentlyBatched
    , mapConcurrentlyBatched_
    , mapConcurrentlyChunks
    , mapConcurrentlyChunks_
      -- * merge strategies
    , mergeConcatAll
    )
where

import Control.Concurrent.Async
import Control.DeepSeq
import Control.Exception
import Control.Monad
import Data.List
import Data.Sequence (Seq)
import qualified Control.Concurrent.QSem as S
import qualified Data.Foldable as F
import qualified Data.Sequence as Seq

-- | Span a green thread for each task, but only execute N tasks
-- concurrently. Ignore the result
mapConcurrentlyBounded_ :: Traversable t => Int -> (a -> IO ()) -> t a -> IO ()
mapConcurrentlyBounded_ bound action =
    void . mapConcurrentlyBounded bound action

-- | Span a green thread for each task, but only execute N tasks
-- concurrently.
mapConcurrentlyBounded :: Traversable t => Int -> (a -> IO b) -> t a -> IO (t b)
mapConcurrentlyBounded bound action items =
    do qs <- S.newQSem bound
       let wrappedAction x =
               bracket_ (S.waitQSem qs) (S.signalQSem qs) (action x)
       mapConcurrently wrappedAction items

-- | Span green threads to perform N (batch size) tasks in one thread
-- and ignore results
mapConcurrentlyBatched_ ::
    (Foldable t) => Int -> (a -> IO ()) -> t a -> IO ()
mapConcurrentlyBatched_ batchSize =
    mapConcurrentlyBatched batchSize (const $ pure ())

-- | Span green threads to perform N (batch size) tasks in one thread
-- and merge results using provided merge function
mapConcurrentlyBatched ::
    (NFData b, Foldable t)
    => Int -> (Seq (Seq b) -> IO r) -> (a -> IO b) -> t a -> IO r
mapConcurrentlyBatched batchSize merge action items =
    do let chunks = chunkList batchSize $ F.toList items
       r <- mapConcurrently (\x -> force <$> mapM action x) chunks
       merge r

-- | Split input into N chunks with equal length and work on
-- each chunk in a dedicated green thread. Ignore results
mapConcurrentlyChunks_ :: (Foldable t) => Int -> (a -> IO ()) -> t a -> IO ()
mapConcurrentlyChunks_ chunkCount =
    mapConcurrentlyChunks chunkCount (const $ pure ())

-- | Split input into N chunks with equal length and work on
-- each chunk in a dedicated green thread. Then merge results using provided merge function
mapConcurrentlyChunks ::
    (NFData b, Foldable t)
    => Int -> (Seq (Seq b) -> IO r) -> (a -> IO b) -> t a -> IO r
mapConcurrentlyChunks chunkCount merge action items =
    do let listSize = F.length items
           batchSize :: Double
           batchSize = fromIntegral listSize / fromIntegral chunkCount
       mapConcurrentlyBatched (ceiling batchSize) merge action items

-- | Chunk a list into chunks of N elements at maximum
chunkList :: forall a. Int -> [a] -> Seq (Seq a)
chunkList chunkSize =
    go 0 Seq.empty
    where
      go :: Int -> Seq a -> [a] -> Seq (Seq a)
      go !size !chunk q
          | size == chunkSize =
                Seq.singleton chunk Seq.>< go 0 Seq.empty q
          | otherwise =
                case q of
                  [] -> Seq.singleton chunk
                  (x : xs) -> go (size + 1) (chunk Seq.|> x) xs

-- | Merge all chunks by combining to one list
mergeConcatAll :: Seq (Seq a) -> [a]
mergeConcatAll = F.toList . foldl' (Seq.><) Seq.empty . F.toList
