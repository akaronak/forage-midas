package com.jpmc.midascore.component;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.client.RestTemplate;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;

@Component
public class TransactionListener {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    private final RestTemplate restTemplate;

    public TransactionListener(RestTemplateBuilder restTemplateBuilder) {
        this.restTemplate = restTemplateBuilder.build();
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction) {

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        float amount = transaction.getAmount();

        if (sender.getBalance() < amount) {
            return;
        }

        Incentive incentive = restTemplate.postForObject("http://localhost:8080/incentive", transaction, Incentive.class);
        float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0;

        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record = new TransactionRecord();
        record.setSender(sender);
        record.setRecipient(recipient);
        record.setAmount(amount);
        record.setIncentive(incentiveAmount);

        transactionRepository.save(record);

        if (sender.getName().equals("wilbur")) {
            System.out.println("WILBUR BALANCE = " + sender.getBalance());
        }
        if (recipient.getName().equals("wilbur")) {
            System.out.println("WILBUR BALANCE = " + recipient.getBalance());
        }
    }
}
