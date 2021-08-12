from gql import gql, Client

QUERIES = {}

QUERIES['main'] = gql(
    """
    query main($block: Int)
    { 
        subgraphVersions(first:1000, block: { number: $block }){
        id
        subgraph{
            id
            createdAt
            displayName
            owner {
            id
            }
            active
        }
        subgraphDeployment{
            id
            stakedTokens
            indexingIndexerRewardAmount
            indexingDelegatorRewardAmount
            queryFeesAmount
            curatorFeeRewards
            signalledTokens
            signalAmount
            pricePerShare
            network {
            id
            }
        }

        }
    }

    """
    )

QUERIES['global'] = gql(
    """
    query global($block: Int)
    { 
        graphNetwork(id: "1", block: { number: $block }){
            curationPercentage
            delegationRatio
            totalTokensStaked
            totalTokensAllocated
            totalDelegatedTokens
            totalQueryFees
            totalCuratorQueryFees
            totalIndexingIndexerRewards
            totalIndexingDelegatorRewards
            totalTokensSignalled
        }
        }
        
    """

    
)

QUERIES['allocations'] = gql(
    """
    query allocQuery($block: Int)
    {
        allocations(block: { number: $block }, first:1000){
            id
            queryFeesCollected
            delegationFees
            subgraphDeployment {
            id
            versions{
                id
            }
            }
            allocatedTokens
            status
            closedAtBlockNumber
        }
    }
    """

    
)