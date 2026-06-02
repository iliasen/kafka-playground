package by.iliasen;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class PartitionDistributionAnalyzer {
    public static void main(String[] args) throws Exception {
        // Конфигурация подключения к кластеру
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        try (AdminClient admin = AdminClient.create(props)) {
            // Получение метаданных кластера
            DescribeClusterResult clusterResult = admin.describeCluster();
            Collection<Node> nodes = clusterResult.nodes().get();

            // Запрос метаданных топика
            String topicName = "orders";
            DescribeTopicsResult topicResult = admin.describeTopics(Collections.singleton(topicName));
            TopicDescription topicDesc = topicResult.topicNameValues().get(topicName).get();

            // Вывод информации о партициях
            System.out.println("Распределение партиций для топика: " + topicName);
            for (TopicPartitionInfo partition : topicDesc.partitions()) {
                System.out.printf(
                        "Partition %d: Leader=%s, Replicas=%s, ISR=%s%n",
                        partition.partition(),
                        partition.leader().id(),
                        formatNodeIds(partition.replicas()),
                        formatNodeIds(partition.isr())
                );
            }

            System.out.println("\n--- Запрос оффсетов для группы my-group ---");

            // Настраиваем спецификацию фильтра для конкретной партиции (orders-0)
            TopicPartition targetPartition = new TopicPartition("orders", 0);
            ListConsumerGroupOffsetsSpec filterSpec = new ListConsumerGroupOffsetsSpec()
                    .topicPartitions(Collections.singletonList(targetPartition));

            Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = new HashMap<>();
            groupSpecs.put("my-group", filterSpec);

            // Получаем оффсеты
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupSpecs)
                    .all().get().get("my-group");

            if (offsets != null && !offsets.isEmpty()) {
                offsets.forEach((tp, offsetMeta) -> {
                    // 1. Проверяем, есть ли вообще сохраненный оффсет для этой партиции
                    if (offsetMeta == null) {
                        System.out.printf("Топик-Партиция: %s | Оффсеты еще не были закомичены для группы 'my-group'%n", tp);
                        return; // Переходим к следующей партиции в map (аналог continue)
                    }

                    // 2. Если оффсет есть, ищем инфо о партиции в метаданных топика
                    Optional<TopicPartitionInfo> partitionInfoOpt = topicDesc.partitions().stream()
                            .filter(p -> p.partition() == tp.partition())
                            .findFirst();

                    if (partitionInfoOpt.isPresent()) {
                        TopicPartitionInfo partitionInfo = partitionInfoOpt.get();
                        System.out.printf("Топик-Партиция: %s | Текущий оффсет группы: %d | ISR: %s%n",
                                tp,
                                offsetMeta.offset(),
                                formatNodeIds(partitionInfo.isr())
                        );
                    } else {
                        System.out.println("Информация о партиции " + tp + " не найдена в метаданных.");
                    }
                });
            } else {
                System.out.println("Оффсеты для группы 'my-group' и партиции orders-0 не найдены. (Возможно, группа еще не вычитывала этот топик)");
            }
        }
    }

    public static void createTopicWithReplicaAssignment() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        try (AdminClient admin = AdminClient.create(props)) {
            Map<Integer, List<Integer>> replicaAssignments = new HashMap<>();
            replicaAssignments.put(0, Arrays.asList(1, 2));  // Partition-0: Leader=1, Follower=2
            replicaAssignments.put(1, Arrays.asList(2, 3));  // Partition-1: Leader=2, Follower=3
            replicaAssignments.put(2, Arrays.asList(3, 1));  // Partition-2: Leader=3, Follower=1

            NewTopic newTopic = new NewTopic("custom-topic", replicaAssignments);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Топик 'custom-topic' успешно создан с ручным распределением реплик!");
        }
    }

    private static String formatNodeIds(List<Node> nodes) {
        if (nodes == null) return "";
        return nodes.stream().map(node -> String.valueOf(node.id())).collect(Collectors.joining(","));
    }
}