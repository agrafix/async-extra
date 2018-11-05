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
import Control.Monad.IO.Unlift
import Data.List.Split (chunksOf)

import qualified Control.Concurrent.QSem as S
import qualified Data.Foldable as F

-- | Span a green thread for each task, but only execute N tasks
-- concurrently. Ignore the result
mapConcurrentlyBounded_ ::
  (MonadUnliftIO m, Traversable t) => Int -> (a -> m ()) -> t a -> m ()
mapConcurrentlyBounded_ bound action items =
  void $ withRunInIO $ \run ->
    mapConcurrentlyBounded bound (run . action) items

-- | Span a green thread for each task, but only execute N tasks
-- concurrently.
mapConcurrentlyBounded :: (MonadUnliftIO m, Traversable t) => Int -> (a -> m b) -> t a -> m (t b)
mapConcurrentlyBounded bound action items = withRunInIO $ \run ->
    do qs <- S.newQSem bound
       let wrappedAction x =
               bracket_ (S.waitQSem qs) (S.signalQSem qs) (run $ action x)
       mapConcurrently wrappedAction items

-- | Span green threads to perform N (batch size) tasks in one thread
-- and ignore results
mapConcurrentlyBatched_ ::
    (MonadUnliftIO m, Foldable t) => Int -> (a -> m ()) -> t a -> m ()
mapConcurrentlyBatched_ batchSize =
    mapConcurrentlyBatched batchSize (const $ pure ())

-- | Span green threads to perform N (batch size) tasks in one thread
-- and merge results using provided merge function
mapConcurrentlyBatched ::
    (MonadUnliftIO m, NFData b, Foldable t)
    => Int -> ([[b]] -> m r) -> (a -> m b) -> t a -> m r
mapConcurrentlyBatched batchSize merge action items = withRunInIO $ \run ->
    do let chunks = chunksOf batchSize $ F.toList items
       r <- mapConcurrently (fmap force . mapM (run . action)) chunks
       run $ merge r

-- | Split input into N chunks with equal length and work on
-- each chunk in a dedicated green thread. Ignore results
mapConcurrentlyChunks_ :: (MonadUnliftIO m, Foldable t) => Int -> (a -> m ()) -> t a -> m ()
mapConcurrentlyChunks_ chunkCount =
    mapConcurrentlyChunks chunkCount (const $ pure ())

-- | Split input into N chunks with equal length and work on
-- each chunk in a dedicated green thread. Then merge results using provided merge function
mapConcurrentlyChunks ::
    (MonadUnliftIO m, NFData b, Foldable t)
    => Int -> ([[b]] -> m r) -> (a -> m b) -> t a -> m r
mapConcurrentlyChunks chunkCount merge action items =
    do let listSize = F.length items
           batchSize :: Double
           batchSize = fromIntegral listSize / fromIntegral chunkCount
       mapConcurrentlyBatched (ceiling batchSize) merge action items

-- | Merge all chunks by combining to one list. (Equiv to 'join')
mergeConcatAll :: [[a]] -> [a]
mergeConcatAll = join
